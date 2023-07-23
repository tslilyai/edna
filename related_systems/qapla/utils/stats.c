/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#include <stdio.h>
#include <stdint.h>

#include "statistics.h"
#include "stats_defs.h"

STATS_DEFS_MAP(DECL_STAT);

#if 0
char *stats_names[MAX_NUM_STATS] = {
	STATS_DEFS_MAP(STATS_STR)
};
#endif

#if STATISTICS > 0
void init_stats()
{
	// API stats
	INIT_STAT(QRM_INIT, "QRM init (ms)", 0.0, 200.0, 0.1);
	INIT_STAT(QRM_RESET, "QRM reset (ms)", 0.0, 50.0, 0.01);
	INIT_STAT(QRM_SCANNER, "QRM scan (ms)", 0.0, 200.0, 0.05);
	INIT_STAT(QRM_PARSER, "QRM parse (ms)", 0.0, 200.0, 0.05);
	INIT_STAT(QRM_POL_EVAL, "QRM pol eval (ms)", 0.0, 200.0, 0.005);
	INIT_STAT(QRM_RESOLVE_POL_PARMS, "QRM resolve pol parms (ms)", 0.0, 100.0, 0.002);
	INIT_STAT(QRM_CACHE_REWRITE, "QRM rewrite cached SQL (ms)", 0.0, 100.0, 0.005);
	INIT_STAT(QRM_REWRITE, "QRM rewrite SQL (ms)", 0.0, 100.0, 0.001);
	INIT_STAT(QRM_TRANSLATE, "QRM translate SQL (ms)", 0.0, 100.0, 0.002);
	INIT_STAT(QRM_CACHE_GET, "QRM query cache lookup (ms)", 0.0, 200.0, 0.001);
	INIT_STAT(QRM_CACHE_SET, "QRM query cache insert (ms)", 0.0, 200.0, 0.001);
	INIT_STAT(QRM_CLONE_CACHE_ELEM, "QRM clone cache elem (ms)", 0.0, 200.0, 0.002);
	INIT_STAT(QRM_CLONE_QSTR_ELEM, "QRM clone qstr elem (ms)", 0.0, 200.0, 0.002);
	INIT_STAT(QRM_SQL_PARSER, "QRM SQL parser (ms)", 0.0, 200.0, 0.1);
	INIT_STAT(QRM_AST_RESOLVE, "QRM gather symbols (ms)", 0.0, 50.0, 0.05);

	// gather per-query statistics into per-iter statistics
	INIT_STAT(QRM_AGGR_SCANNER, "QRM per-iter scan (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_PARSER, "QRM per-iter parse (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_POL_EVAL, "QRM per-iter pol eval (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_RESOLVE_POL_PARMS, "QRM per-iter resolve pol params (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_CACHE_REWRITE, "QRM per-iter rewrite cached SQL (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_REWRITE, "QRM per-iter rewrite SQL (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_TRANSLATE, "QRM per-iter translate SQL (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_CACHE_GET, "QRM per-iter query cache lookup (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_CACHE_SET, "QRM per-iter query cache insert (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_CLONE_CACHE_ELEM, "QRM per-iter clone cache elem (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_CLONE_QSTR_ELEM, "QRM per-iter clone qstr elem (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_SQL_PARSER, "QRM per-iter SQL parser (ms)", 0.0, 200.0, 0.5);
	INIT_STAT(QRM_AGGR_AST_RESOLVE, "QRM per-iter gather symbol (ms)", 0.0, 200.0, 0.5);

	// fine-grained parser stats
	INIT_STAT(SQL_PARSER_INIT, "SQL parser init (ms)", 0.0, 50.0, 0.1);
	INIT_STAT(SQL_PARSER_DEL, "SQL parser delete (ms)", 0.0, 50.0, 0.1);
	INIT_STAT(MYSQL_RESET, "mysql reset (ms)", 0.0, 50.0, 0.1);
	INIT_STAT(MYSQL_ANTLR_STREAM_INIT, "mysql parser stream init (ms)", 0.0, 50.0, 0.1);
	INIT_STAT(MYSQL_INPUT_PROCESS, "mysql parser process input (ms)", 0.0, 50.0, 0.1);
	INIT_STAT(MYSQL_INIT_LEXER, "mysql parser init lexer (ms)", 0.0, 50.0, 0.1);
	INIT_STAT(MYSQL_GEN_TOKEN, "mysql parser gen token (ms)", 0.0, 50.0, 0.1);
	INIT_STAT(MYSQL_INIT_PARSER, "mysql parser init parser (ms)", 0.0, 50.0, 0.1);
	INIT_STAT(MYSQL_RUN_QUERY, "mysql parser run query (ms)", 0.0, 200.0, 0.1);
	INIT_STAT(MYSQL_INIT_RESET, "mysql parser reset all (ms)", 0.0, 50.0, 0.1);

	// benchmark stats
	INIT_STAT(RUNTIME, "total runtime (ms)", 0.0, 50.0, 0.1);
	INIT_STAT(ITER_RUNTIME, "iter runtime (ms)", 0.0, 200.0, 0.1);
	INIT_STAT(QUERY_RUNTIME, "per query runtime (ms)", 0.0, 200.0, 0.1);
	INIT_STAT(SQL_EXEC, "query exec (ms)", 0.0, 200.0, 0.1);
	INIT_STAT(SQL_RESULT, "query fetch result (ms)", 0.0, 50.0, 0.1);
	INIT_STAT(RESULT_COUNT, "#rows (count)", 0.0, 100000.0, 1.0);
}

void dest_stats()
{
	STATS_DEFS_MAP(DEST_STAT)
}

void reset_stats()
{
	STATS_DEFS_MAP(RESET_STAT)
}

void print_stats(FILE *f, int full)
{
	STATS_DEFS_MAP(STATS_PRINT);
}

#else

#endif
