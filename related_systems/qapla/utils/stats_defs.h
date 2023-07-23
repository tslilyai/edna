/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __STATS_DEFS_H__
#define __STATS_DEFS_H__

#include <stdio.h>
#include <stdint.h>

#include "common/config.h"

#ifdef __cplusplus
extern "C" {
#endif

#define STATS_ENUM(X)	X ,

#define STATS_STR(X)	stats_##X ,
#define STATS_PRINT(X)	PRINT_STAT(X, f, full);

#define STATS_DEFS_MAP(Y)	\
	Y(QRM_INIT)	\
	Y(QRM_RESET)	\
	Y(QRM_SCANNER)	\
	Y(QRM_PARSER)	\
	Y(QRM_POL_EVAL)	\
	Y(QRM_RESOLVE_POL_PARMS)	\
	Y(QRM_CACHE_REWRITE)	\
	Y(QRM_REWRITE)	\
	Y(QRM_TRANSLATE)	\
	Y(QRM_CACHE_GET)	\
	Y(QRM_CACHE_SET)	\
	Y(QRM_CLONE_CACHE_ELEM)	\
	Y(QRM_CLONE_QSTR_ELEM)	\
	Y(QRM_SQL_PARSER)	\
	Y(QRM_AST_RESOLVE)	\
	Y(QRM_AGGR_SCANNER)	\
	Y(QRM_AGGR_PARSER)	\
	Y(QRM_AGGR_POL_EVAL)	\
	Y(QRM_AGGR_RESOLVE_POL_PARMS)	\
	Y(QRM_AGGR_CACHE_REWRITE)	\
	Y(QRM_AGGR_REWRITE)	\
	Y(QRM_AGGR_TRANSLATE)	\
	Y(QRM_AGGR_CACHE_GET)	\
	Y(QRM_AGGR_CACHE_SET)	\
	Y(QRM_AGGR_CLONE_CACHE_ELEM)	\
	Y(QRM_AGGR_CLONE_QSTR_ELEM)	\
	Y(QRM_AGGR_SQL_PARSER)	\
	Y(QRM_AGGR_AST_RESOLVE)	\
	Y(SQL_PARSER_INIT)	\
	Y(SQL_PARSER_DEL)	\
	Y(MYSQL_RESET)	\
	Y(MYSQL_ANTLR_STREAM_INIT)	\
	Y(MYSQL_INPUT_PROCESS)	\
	Y(MYSQL_INIT_LEXER)	\
	Y(MYSQL_GEN_TOKEN)	\
	Y(MYSQL_INIT_PARSER)	\
	Y(MYSQL_RUN_QUERY)	\
	Y(MYSQL_INIT_RESET)	\
	Y(RUNTIME)	\
	Y(ITER_RUNTIME)	\
	Y(QUERY_RUNTIME)	\
	Y(SQL_EXEC)	\
	Y(SQL_RESULT)	\
	Y(RESULT_COUNT)	\

#if STATISTICS > 0

typedef enum {
	STATS_DEFS_MAP(STATS_ENUM)
	MAX_NUM_STATS
} stats_list;

void init_stats();
void dest_stats();
void reset_stats();
void print_stats(FILE *f, int full);
//extern char *stats_names[MAX_NUM_STATS];

#else

#define init_stats()

#define dest_stats()

#define reset_stats()

#define print_stats(f, full)

#endif

#ifdef __cplusplus
}
#endif

#endif /* __STATS_DEFS_H__ */
