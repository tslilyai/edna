/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */

#ifndef __STATISTICS_H__
#define __STATISTICS_H__

#include <stdio.h>
#include <malloc.h>

#include "common/config.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct qstat
{
  char name[64]; //variable name

  double start;
  double end;
  double step;

  double sum, sqr_sum;
	unsigned long long iter_count;
  unsigned long long total_count;

  double min;
  double max;

  int bucket_count;
  unsigned long long *distribution; //the distribution
} stat_t;

/**
 * =========== Timing
 */
#if STATISTICS > 0

#include <sys/time.h>

static inline double get_time() {
  struct timeval t;
  gettimeofday(&t, NULL);
  return (double)t.tv_sec * 1000.0 + (double) t.tv_usec/1000.0;
}

void
stat_add_data_point(stat_t *stats, double value);

void
stat_init(stat_t *stats, char* name, double start, double end, double step);

void
stat_print_summary(stat_t *stats, FILE * fd);

void
stat_print_distribution(stat_t *stats, FILE * fd);

void
stat_reset(stat_t *stats);

void
stat_destroy(stat_t *stats);

#define INIT_TIMER(name) double t_start_##name = 0.0, t_end_##name = 0.0
#define START_TIMER(name) t_start_##name = get_time()
#define END_TIMER(name) t_end_##name = get_time()
#define START_TIME(name) (t_start_##name)
#define END_TIME(name) (t_end_##name)
#define SPENT_TIME(name) (END_TIME(name) - START_TIME(name))

#define DECL_STAT_EXTERN(name)	extern stat_t *stats_##name;
#define DECL_STAT(name)	stat_t *stats_##name = NULL;
#define STAT_NAME(name)	stats_##name

#define INIT_STAT(name, desc, sta, end, st)			\
	stats_##name = (stat_t *) malloc(sizeof(stat_t));			\
	stat_init(stats_##name, desc, sta, end, st)

#define DEST_STAT(name)					\
	if(stats_##name != NULL) {				\
		stat_destroy(stats_##name);			\
		free(stats_##name);				\
		stats_##name = NULL;	\
	}

#define RESET_STAT(name)					\
	if(stats_##name != NULL) {				\
		stat_reset(stats_##name);			\
	}

#define ADD_TIME_POINT(name)					\
	if(stats_##name != NULL) {				\
	    (stat_add_data_point(stats_##name, SPENT_TIME(name))); \
	}

#define ADD_COUNT_POINT(name, val)				\
	if(stats_##name != NULL) {				\
	    (stat_add_data_point(stats_##name, val));		\
	}

#define STAT_START_TIMER(name)	\
	INIT_TIMER(name); START_TIMER(name)

#define STAT_END_TIMER(name)	\
	END_TIMER(name); ADD_TIME_POINT(name)

#define PRINT_STAT(name, f, full)	\
	do {	\
		if (full)	\
			stat_print_distribution(stats_##name, f);	\
		else	\
			stat_print_summary(stats_##name, f);	\
	} while (0)

#define GET_STAT_FIELD(name, field)	\
	(stats_##name != NULL ? (stats_##name)->field : 0)

#define SET_STAT_FIELD(name, field, val)	\
	if (stats_##name != NULL) {	\
		(stats_##name)->field = (val);	\
	}

#else

#define stat_add_data_point(args...)

#define get_time(args...)

#define stat_add_data_point(args...)

#define stat_init(args...)

#define stat_print_summary(args...)

#define stat_print_distribution(args...)

#define stat_reset(args...)

#define stat_destroy(args...)

#define INIT_TIMER(name)
#define START_TIMER(name)
#define END_TIMER(name)
#define START_TIME(name) 0
#define END_TIME(name) 0
#define SPENT_TIME(name) 0

#define DECL_STAT_EXTERN(name)
#define DECL_STAT(name)

#define INIT_STAT(name, desc, sta, end, st)
#define DEST_STAT(name)
#define RESET_STAT(name)

#define ADD_TIME_POINT(name)
#define ADD_COUNT_POINT(name, val)

#define STAT_START_TIMER(name)
#define STAT_END_TIMER(name)

#define PRINT_STAT(name, f, full)

#define GET_STAT_FIELD(name, field)
#define SET_STAT_FILED(name, field, val)

#endif

#ifdef __cplusplus
}
#endif

#endif /* __STATISTICS_H__ */
