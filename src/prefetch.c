/* prefetch.c

Copyright (C) 2026 Mark Kusper.

Background read-ahead for slow (network / FUSE) filesystems.

A small pool of worker threads copies the next few images in slideshow
order into a per-process directory on a local tmpfs.  The main thread then
loads from the local copy instead of the slow source, so a keypress does not
pay the source's round-trip latency.  Workers only do file I/O and touch the
shared index under a mutex; Imlib2 and X are never called off the main thread.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies of the Software and its documentation and acknowledgment shall be
given in the documentation and software packages that this Software was
used.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

#include "feh.h"
#include "filelist.h"
#include "options.h"
#include "prefetch.h"

#include <pthread.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <dirent.h>
#include <signal.h>

enum { PF_QUEUED, PF_LOADING, PF_READY, PF_FAILED };

typedef struct pf_entry {
	char *name;		/* source path, as in the filelist */
	char *local;		/* local copy, valid when state == PF_READY */
	off_t size;		/* source size and mtime when the copy was taken */
	struct timespec mtime;
	int state;
	unsigned wanted;	/* generation that last asked for this entry */
	struct pf_entry *next;
} pf_entry;

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t work_cv = PTHREAD_COND_INITIALIZER;	/* queue became non-empty */
static pthread_cond_t done_cv = PTHREAD_COND_INITIALIZER;	/* some entry finished */

static pf_entry *entries = NULL;	/* insertion order, oldest first */
static int n_entries = 0;
static pf_entry **queue = NULL;		/* pending work, highest priority first */
static int q_len = 0, q_cap = 0;

static char cache_dir[PATH_MAX];
static unsigned generation = 0;
static unsigned long seq = 0;
static int initialized = 0;
static volatile int shutting_down = 0;

#define PF_MAX_THREADS 6

static int copy_file(const char *src, const char *dst, struct stat *st)
{
	static __thread char buf[256 * 1024];
	int in, out, ok = 1;
	ssize_t n;

	if ((in = open(src, O_RDONLY)) < 0)
		return 0;
	if (fstat(in, st) != 0 || (out = open(dst, O_WRONLY | O_CREAT | O_EXCL, 0600)) < 0) {
		close(in);
		return 0;
	}
	while ((n = read(in, buf, sizeof(buf))) > 0) {
		char *p = buf;
		while (n > 0) {
			ssize_t w = write(out, p, n);
			if (w < 0) {
				if (errno == EINTR)
					continue;
				ok = 0;
				break;
			}
			p += w;
			n -= w;
		}
		if (!ok)
			break;
	}
	if (n < 0)
		ok = 0;
	close(in);
	if (close(out) < 0)
		ok = 0;
	if (!ok)
		unlink(dst);
	return ok;
}

static void *worker(__attribute__((unused)) void *arg)
{
	for (;;) {
		pf_entry *e;
		char *name, local[PATH_MAX + 64];
		const char *base, *ext;
		struct stat st;
		int ok;

		pthread_mutex_lock(&lock);
		while (q_len == 0)
			pthread_cond_wait(&work_cv, &lock);
		e = queue[0];
		q_len--;
		memmove(queue, queue + 1, q_len * sizeof(*queue));
		e->state = PF_LOADING;
		name = estrdup(e->name);
		/* keep the extension so Imlib2 picks the right loader */
		base = strrchr(name, '/');
		base = base ? base + 1 : name;
		ext = strrchr(base, '.');
		snprintf(local, sizeof(local), "%s/%lu%s", cache_dir, ++seq, ext ? ext : "");
		pthread_mutex_unlock(&lock);

		ok = shutting_down ? 0 : copy_file(name, local, &st);

		pthread_mutex_lock(&lock);
		if (ok) {
			e->local = estrdup(local);
			e->size = st.st_size;
			e->mtime = st.st_mtim;
			e->state = PF_READY;
		} else {
			e->state = PF_FAILED;
		}
		pthread_cond_broadcast(&done_cv);
		pthread_mutex_unlock(&lock);
		free(name);
	}
	return NULL;
}

static pf_entry *find_entry(const char *name)
{
	pf_entry *e;
	for (e = entries; e; e = e->next)
		if (strcmp(e->name, name) == 0)
			return e;
	return NULL;
}

static void free_entry(pf_entry *e)
{
	if (e->local) {
		unlink(e->local);
		free(e->local);
	}
	free(e->name);
	free(e);
}

static void unlink_entry(pf_entry *victim)
{
	pf_entry **pp;
	for (pp = &entries; *pp; pp = &(*pp)->next) {
		if (*pp == victim) {
			*pp = victim->next;
			n_entries--;
			return;
		}
	}
}

static void queue_push(pf_entry *e)
{
	if (q_len == q_cap) {
		q_cap = q_cap ? q_cap * 2 : 16;
		queue = erealloc(queue, q_cap * sizeof(*queue));
	}
	queue[q_len++] = e;
}

/* Ask for one file; creates and queues an entry unless it is already known. */
static void want(const char *name)
{
	pf_entry *e = find_entry(name);
	if (e) {
		e->wanted = generation;
		return;
	}
	e = emalloc(sizeof(*e));
	memset(e, 0, sizeof(*e));
	e->name = estrdup((char *) name);
	e->state = PF_QUEUED;
	e->wanted = generation;
	/* append, so that entries stay in insertion order for eviction */
	if (!entries) {
		entries = e;
	} else {
		pf_entry *t = entries;
		while (t->next)
			t = t->next;
		t->next = e;
	}
	n_entries++;
	queue_push(e);
}

/* Remove every file in dir and then dir itself. */
static void remove_dir(const char *dir)
{
	DIR *d;
	struct dirent *de;
	char path[PATH_MAX + 256];

	if ((d = opendir(dir))) {
		while ((de = readdir(d))) {
			if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
				continue;
			snprintf(path, sizeof(path), "%s/%s", dir, de->d_name);
			unlink(path);
		}
		closedir(d);
	}
	rmdir(dir);
}

static void cleanup(void)
{
	/* Workers may still be writing a copy; sweeping the directory catches
	 * those too, and a copy created after the sweep fails on open(). */
	int i;

	pthread_mutex_lock(&lock);
	shutting_down = 1;
	q_len = 0;
	entries = NULL;
	pthread_mutex_unlock(&lock);
	for (i = 0; i < 5; i++) {
		remove_dir(cache_dir);
		if (access(cache_dir, F_OK) != 0)
			break;
		usleep(20000);
	}
}

/* Purge cache directories left behind by feh processes that no longer exist
 * (e.g. killed with SIGKILL). */
static void purge_stale(const char *base)
{
	DIR *d;
	struct dirent *de;
	char path[PATH_MAX + 256];
	long pid;

	if (!(d = opendir(base)))
		return;
	while ((de = readdir(d))) {
		if (strncmp(de->d_name, "feh-prefetch-", 13) != 0)
			continue;
		pid = atol(de->d_name + 13);
		if (pid <= 0 || pid == getpid())
			continue;
		if (kill((pid_t) pid, 0) == -1 && errno == ESRCH) {
			snprintf(path, sizeof(path), "%s/%s", base, de->d_name);
			remove_dir(path);
		}
	}
	closedir(d);
}

void prefetch_init(void)
{
	const char *base;
	pthread_t tid;
	pthread_attr_t attr;
	int i, threads;

	if (initialized || opt.prefetch <= 0)
		return;

	base = getenv("XDG_RUNTIME_DIR");
	if (!base || access(base, W_OK) != 0)
		base = access("/dev/shm", W_OK) == 0 ? "/dev/shm" : (getenv("TMPDIR") ? getenv("TMPDIR") : "/tmp");
	purge_stale(base);
	snprintf(cache_dir, sizeof(cache_dir), "%s/feh-prefetch-%d", base, (int) getpid());
	if (mkdir(cache_dir, 0700) != 0 && errno != EEXIST) {
		weprintf("prefetch: cannot create %s, read-ahead disabled:", cache_dir);
		opt.prefetch = 0;
		return;
	}

	threads = opt.prefetch < PF_MAX_THREADS ? opt.prefetch : PF_MAX_THREADS;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	for (i = 0; i < threads; i++) {
		if (pthread_create(&tid, &attr, worker, NULL) != 0) {
			if (i == 0) {
				weprintf("prefetch: cannot start worker thread, read-ahead disabled:");
				opt.prefetch = 0;
				rmdir(cache_dir);
				pthread_attr_destroy(&attr);
				return;
			}
			break;
		}
	}
	pthread_attr_destroy(&attr);
	atexit(cleanup);
	initialized = 1;
}

/* Called from the main thread whenever the current image changes. */
void prefetch_schedule(gib_list *cur)
{
	gib_list *l;
	pf_entry *e, *next;
	int i, n_fwd, n_back, max_entries;

	if (!initialized || !cur || !filelist)
		return;

	n_fwd = opt.prefetch;
	n_back = opt.prefetch / 4;
	if (n_back < 1)
		n_back = 1;
	max_entries = opt.prefetch * 8 + 32;

	pthread_mutex_lock(&lock);
	generation++;

	/* Pending work from the previous position is stale: drop it. Entries
	 * that are in flight are left alone and simply finish. */
	q_len = 0;
	for (e = entries; e; e = next) {
		next = e->next;
		if (e->state == PF_QUEUED) {
			unlink_entry(e);
			free_entry(e);
		}
	}

	/* keep the current image's copy around for reloads and back-steps */
	if ((e = find_entry(FEH_FILE(cur->data)->filename)))
		e->wanted = generation;

	l = cur;
	for (i = 0; i < n_fwd; i++) {
		l = l->next ? l->next : filelist;
		if (l == cur)
			break;
		want(FEH_FILE(l->data)->filename);
	}
	l = cur;
	for (i = 0; i < n_back; i++) {
		l = l->prev ? l->prev : gib_list_last(filelist);
		if (l == cur)
			break;
		want(FEH_FILE(l->data)->filename);
	}

	/* evict oldest finished entries nobody asked for this round */
	for (e = entries; e && n_entries > max_entries; e = next) {
		next = e->next;
		if (e->wanted != generation && (e->state == PF_READY || e->state == PF_FAILED)) {
			unlink_entry(e);
			free_entry(e);
		}
	}

	if (q_len)
		pthread_cond_broadcast(&work_cv);
	pthread_mutex_unlock(&lock);
}

/* Returns a malloc'd path to a local copy of name, waiting for an in-flight
 * fetch to finish, or NULL if the file is not (or could not be) prefetched. */
char *prefetch_local_copy(const char *name)
{
	pf_entry *e;
	char *ret = NULL;
	int i;

	if (!initialized)
		return NULL;

	pthread_mutex_lock(&lock);
	e = find_entry(name);
	if (e && e->state == PF_QUEUED) {
		/* the user outran the read-ahead: serve this one next */
		for (i = 0; i < q_len; i++) {
			if (queue[i] == e) {
				memmove(queue + 1, queue, i * sizeof(*queue));
				queue[0] = e;
				break;
			}
		}
		pthread_cond_broadcast(&work_cv);
	}
	while (e && (e->state == PF_QUEUED || e->state == PF_LOADING))
		pthread_cond_wait(&done_cv, &lock);
	if (e && e->state == PF_READY)
		ret = estrdup(e->local);
	pthread_mutex_unlock(&lock);
	return ret;
}

/* Forget any copy of name, so that the next load reads the source again. */
void prefetch_invalidate(const char *name)
{
	pf_entry *e;

	if (!initialized)
		return;

	pthread_mutex_lock(&lock);
	e = find_entry(name);
	if (e && (e->state == PF_READY || e->state == PF_FAILED)) {
		unlink_entry(e);
		free_entry(e);
	}
	pthread_mutex_unlock(&lock);
}

/* Drop the copy of name only if the source has changed since it was taken
 * (size or mtime differ, or it cannot be stat'd). One stat instead of a full
 * re-read when a reload finds the file unchanged. */
void prefetch_revalidate(const char *name)
{
	pf_entry *e;
	struct stat st;
	off_t size;
	struct timespec mtime;
	int have = 0;

	if (!initialized)
		return;

	pthread_mutex_lock(&lock);
	e = find_entry(name);
	if (e && e->state == PF_READY) {
		size = e->size;
		mtime = e->mtime;
		have = 1;
	}
	pthread_mutex_unlock(&lock);

	if (!have)
		return;		/* nothing cached, or still in flight: nothing to keep stale */

	/* stat without holding the lock: this may take a round trip */
	if (stat(name, &st) == 0 && st.st_size == size
			&& st.st_mtim.tv_sec == mtime.tv_sec && st.st_mtim.tv_nsec == mtime.tv_nsec)
		return;

	prefetch_invalidate(name);
}
