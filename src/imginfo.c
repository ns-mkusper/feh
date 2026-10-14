/* imginfo.c

Copyright (C) 2026 Mark Kusper.

Fast image dimensions from file headers, gathered in parallel.

Sorting by width, height or pixels needs every image's dimensions before
anything can be shown. feh used to get them by fully decoding every file,
which on a network filesystem means reading the whole directory through a
high-latency link before the first image appears. This module reads only the
first few kilobytes of each file, parses the header of the common formats
(JPEG, PNG, GIF, WebP, BMP) and fills in feh_file_info directly, using a pool
of threads so that per-file latency overlaps. Files it cannot parse are left
untouched and fall back to the full load.

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
#include "imginfo.h"
#include "signals.h"

#include <pthread.h>
#include <fcntl.h>
#include <sys/stat.h>

#define FIRST_READ	(64 * 1024)
#define MAX_READ	(2 * 1024 * 1024)
#define MAX_THREADS	16

static unsigned be16(const unsigned char *p) { return (p[0] << 8) | p[1]; }
static unsigned le16(const unsigned char *p) { return p[0] | (p[1] << 8); }
static unsigned be32(const unsigned char *p) { return ((unsigned) p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3]; }
static unsigned le32(const unsigned char *p) { return ((unsigned) p[3] << 24) | (p[2] << 16) | (p[1] << 8) | p[0]; }

/* Returns 1 on success, 0 on "not this format / malformed", -1 if more data
 * might help (JPEG whose SOF marker lies beyond the buffer). */
static int parse_jpeg(const unsigned char *b, size_t n, int *w, int *h)
{
	size_t i = 2;

	if (n < 4 || b[0] != 0xFF || b[1] != 0xD8)
		return 0;
	while (i + 4 <= n) {
		unsigned marker, len;
		if (b[i] != 0xFF) {
			i++;
			continue;
		}
		marker = b[i + 1];
		if (marker == 0xFF) {	/* fill byte */
			i++;
			continue;
		}
		if (marker == 0xD8 || marker == 0x01 || (marker >= 0xD0 && marker <= 0xD7)) {
			i += 2;	/* standalone marker, no length */
			continue;
		}
		if (marker == 0xD9 || marker == 0xDA)
			return 0;	/* EOI / start of scan without SOF: give up */
		len = be16(b + i + 2);
		if (len < 2)
			return 0;
		if ((marker >= 0xC0 && marker <= 0xC3) || (marker >= 0xC5 && marker <= 0xC7)
				|| (marker >= 0xC9 && marker <= 0xCB) || (marker >= 0xCD && marker <= 0xCF)) {
			if (i + 9 > n)
				return -1;
			*h = be16(b + i + 5);
			*w = be16(b + i + 7);
			return (*w > 0 && *h > 0) ? 1 : 0;
		}
		i += 2 + len;
	}
	return -1;
}

static int parse_header(const unsigned char *b, size_t n, int *w, int *h, int *alpha, const char **fmt)
{
	*alpha = 0;
	if (n >= 24 && memcmp(b, "\x89PNG\r\n\x1a\n", 8) == 0 && memcmp(b + 12, "IHDR", 4) == 0) {
		*w = be32(b + 16);
		*h = be32(b + 20);
		if (n >= 26)
			*alpha = (b[25] == 4 || b[25] == 6);
		*fmt = "png";
		return (*w > 0 && *h > 0);
	}
	if (n >= 10 && memcmp(b, "GIF8", 4) == 0) {
		*w = le16(b + 6);
		*h = le16(b + 8);
		*fmt = "gif";
		return (*w > 0 && *h > 0);
	}
	if (n >= 30 && memcmp(b, "RIFF", 4) == 0 && memcmp(b + 8, "WEBP", 4) == 0) {
		*fmt = "webp";
		if (memcmp(b + 12, "VP8 ", 4) == 0) {
			*w = le16(b + 26) & 0x3FFF;
			*h = le16(b + 28) & 0x3FFF;
		} else if (memcmp(b + 12, "VP8L", 4) == 0) {
			unsigned bits = le32(b + 21);
			*w = (bits & 0x3FFF) + 1;
			*h = ((bits >> 14) & 0x3FFF) + 1;
			*alpha = (bits >> 28) & 1;
		} else if (memcmp(b + 12, "VP8X", 4) == 0) {
			*w = (b[24] | (b[25] << 8) | (b[26] << 16)) + 1;
			*h = (b[27] | (b[28] << 8) | (b[29] << 16)) + 1;
			*alpha = (b[20] >> 4) & 1;
		} else
			return 0;
		return (*w > 0 && *h > 0);
	}
	if (n >= 26 && b[0] == 'B' && b[1] == 'M') {
		int hh = (int) le32(b + 22);
		*w = (int) le32(b + 18);
		*h = hh < 0 ? -hh : hh;
		*fmt = "bmp";
		return (*w > 0 && *h > 0);
	}
	if (n >= 4 && b[0] == 0xFF && b[1] == 0xD8) {
		*fmt = "jpeg";
		return parse_jpeg(b, n, w, h);
	}
	return 0;
}

/* Fill file->info from the header alone. Returns 0 on success. */
static int probe(feh_file *file)
{
	unsigned char *buf;
	size_t have = 0, want = FIRST_READ;
	int fd, w = 0, h = 0, alpha = 0, r = 0;
	const char *fmt = NULL;

	if (feh_file_stat(file))
		return 1;
	if ((fd = open(file->filename, O_RDONLY)) < 0)
		return 1;
	buf = emalloc(MAX_READ);
	for (;;) {
		while (have < want) {
			ssize_t n = read(fd, buf + have, want - have);
			if (n <= 0)
				break;
			have += n;
		}
		r = parse_header(buf, have, &w, &h, &alpha, &fmt);
		if (r != -1 || have < want || want >= MAX_READ)
			break;
		want = want * 4 > MAX_READ ? MAX_READ : want * 4;
	}
	close(fd);
	free(buf);
	if (r != 1)
		return 1;

	file->info = feh_file_info_new();
	file->info->width = w;
	file->info->height = h;
	file->info->pixels = w * h;
	file->info->has_alpha = alpha;
	file->info->format = estrdup((char *) fmt);
	return 0;
}

struct job {
	gib_list **items;
	int n, next;
	pthread_mutex_t lock;
};

static void *worker(void *arg)
{
	struct job *j = arg;
	for (;;) {
		gib_list *l;
		pthread_mutex_lock(&j->lock);
		if (j->next >= j->n || sig_exit) {
			pthread_mutex_unlock(&j->lock);
			return NULL;
		}
		l = j->items[j->next++];
		pthread_mutex_unlock(&j->lock);
		probe(FEH_FILE(l->data));
	}
}

void feh_fast_info_preload(gib_list *list)
{
	struct job j;
	gib_list *l;
	pthread_t tid[MAX_THREADS];
	int i, n = 0, threads;

	for (l = list; l; l = l->next)
		if (!FEH_FILE(l->data)->info && !path_is_url(FEH_FILE(l->data)->filename))
			n++;
	if (!n)
		return;
	j.items = emalloc(n * sizeof(*j.items));
	j.n = n;
	j.next = 0;
	pthread_mutex_init(&j.lock, NULL);
	for (l = list, i = 0; l; l = l->next)
		if (!FEH_FILE(l->data)->info && !path_is_url(FEH_FILE(l->data)->filename))
			j.items[i++] = l;

	threads = n < MAX_THREADS ? n : MAX_THREADS;
	for (i = 0; i < threads; i++)
		if (pthread_create(&tid[i], NULL, worker, &j) != 0) {
			threads = i;
			break;
		}
	if (threads == 0)
		worker(&j);	/* no threads available: do it inline */
	for (i = 0; i < threads; i++)
		pthread_join(tid[i], NULL);
	pthread_mutex_destroy(&j.lock);
	free(j.items);
}
