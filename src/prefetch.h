/* prefetch.h -- background read-ahead for slow filesystems, see prefetch.c */
#ifndef PREFETCH_H
#define PREFETCH_H

#include "gib_list.h"

void prefetch_init(void);
void prefetch_schedule(gib_list *cur);
char *prefetch_local_copy(const char *name);
void prefetch_invalidate(const char *name);

#endif
