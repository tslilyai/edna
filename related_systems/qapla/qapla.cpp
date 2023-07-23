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
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "qapla.h"
#include "qinfo/query_info.h"
#include "qinfo/query_info_int.h"
#include "sql_pi/sql_rewrite.h"
#include "common/config.h"
//#include "hotcrp/hotcrp_db.h"
#include "utils/statistics.h"
#include "utils/stats_defs.h"

STATS_DEFS_MAP(DECL_STAT_EXTERN);

void
init_query_info(query_info_t *qinfo, const char *schema_name, const char *logname,
		app_info_t *ai)
{
	_init_query_info(qinfo, schema_name, logname, DB_MYSQL, ai);
}

void 
_init_query_info(query_info_t *qinfo, const char *schema_name, const char *logname,
		int db_type, app_info_t *ai)
{
	init_stats();
	STAT_START_TIMER(QRM_INIT);
	_do_init_query_info(qinfo, schema_name, logname, db_type, ai);
	STAT_END_TIMER(QRM_INIT);
}

void
reset_query_info(query_info_t *qinfo)
{
	STAT_START_TIMER(QRM_RESET);
	do_reset_query_info(qinfo);
	STAT_END_TIMER(QRM_RESET);
}

void
flush_query_info_cache(query_info_t *qinfo)
{
	do_reset_query_cache(qinfo);
	ADD_COUNT_POINT(QRM_AGGR_SCANNER, GET_STAT_FIELD(QRM_SCANNER, sum));
	ADD_COUNT_POINT(QRM_AGGR_PARSER, GET_STAT_FIELD(QRM_PARSER, sum));
	ADD_COUNT_POINT(QRM_AGGR_POL_EVAL, GET_STAT_FIELD(QRM_POL_EVAL, sum));
	ADD_COUNT_POINT(QRM_AGGR_RESOLVE_POL_PARMS, GET_STAT_FIELD(QRM_RESOLVE_POL_PARMS, sum));
	ADD_COUNT_POINT(QRM_AGGR_CACHE_REWRITE, GET_STAT_FIELD(QRM_CACHE_REWRITE, sum));
	ADD_COUNT_POINT(QRM_AGGR_REWRITE, GET_STAT_FIELD(QRM_REWRITE, sum));
	ADD_COUNT_POINT(QRM_AGGR_TRANSLATE, GET_STAT_FIELD(QRM_TRANSLATE, sum));
	ADD_COUNT_POINT(QRM_AGGR_CACHE_GET, GET_STAT_FIELD(QRM_CACHE_GET, sum));
	ADD_COUNT_POINT(QRM_AGGR_CACHE_SET, GET_STAT_FIELD(QRM_CACHE_SET, sum));
	ADD_COUNT_POINT(QRM_AGGR_CLONE_CACHE_ELEM, GET_STAT_FIELD(QRM_CLONE_CACHE_ELEM, sum));
	ADD_COUNT_POINT(QRM_AGGR_CLONE_QSTR_ELEM, GET_STAT_FIELD(QRM_CLONE_QSTR_ELEM, sum));
	ADD_COUNT_POINT(QRM_AGGR_SQL_PARSER, GET_STAT_FIELD(QRM_SQL_PARSER, sum));
	ADD_COUNT_POINT(QRM_AGGR_AST_RESOLVE, GET_STAT_FIELD(QRM_AST_RESOLVE, sum));

	SET_STAT_FIELD(QRM_AGGR_SCANNER, iter_count, GET_STAT_FIELD(QRM_SCANNER, total_count));
	SET_STAT_FIELD(QRM_AGGR_PARSER, iter_count, GET_STAT_FIELD(QRM_PARSER, total_count));
	SET_STAT_FIELD(QRM_AGGR_POL_EVAL, iter_count, GET_STAT_FIELD(QRM_POL_EVAL, total_count));
	SET_STAT_FIELD(QRM_AGGR_RESOLVE_POL_PARMS, iter_count, GET_STAT_FIELD(QRM_RESOLVE_POL_PARMS, total_count));
	SET_STAT_FIELD(QRM_AGGR_CACHE_REWRITE, iter_count, GET_STAT_FIELD(QRM_CACHE_REWRITE, total_count));
	SET_STAT_FIELD(QRM_AGGR_REWRITE, iter_count, GET_STAT_FIELD(QRM_REWRITE, total_count));
	SET_STAT_FIELD(QRM_AGGR_TRANSLATE, iter_count, GET_STAT_FIELD(QRM_TRANSLATE, total_count));
	SET_STAT_FIELD(QRM_AGGR_CACHE_GET, iter_count, GET_STAT_FIELD(QRM_CACHE_GET, total_count));
	SET_STAT_FIELD(QRM_AGGR_CACHE_SET, iter_count, GET_STAT_FIELD(QRM_CACHE_SET, total_count));
	SET_STAT_FIELD(QRM_AGGR_CLONE_CACHE_ELEM, iter_count, GET_STAT_FIELD(QRM_CLONE_CACHE_ELEM, total_count));
	SET_STAT_FIELD(QRM_AGGR_CLONE_QSTR_ELEM, iter_count, GET_STAT_FIELD(QRM_CLONE_QSTR_ELEM, total_count));
	SET_STAT_FIELD(QRM_AGGR_SQL_PARSER, iter_count, GET_STAT_FIELD(QRM_SQL_PARSER, total_count));
	SET_STAT_FIELD(QRM_AGGR_AST_RESOLVE, iter_count, GET_STAT_FIELD(QRM_AST_RESOLVE, total_count));

	RESET_STAT(QRM_SCANNER);
	RESET_STAT(QRM_PARSER);
	RESET_STAT(QRM_POL_EVAL);
	RESET_STAT(QRM_RESOLVE_POL_PARMS);
	RESET_STAT(QRM_CACHE_REWRITE);
	RESET_STAT(QRM_REWRITE);
	RESET_STAT(QRM_TRANSLATE);
	RESET_STAT(QRM_CACHE_GET);
	RESET_STAT(QRM_CACHE_SET);
	RESET_STAT(QRM_CLONE_CACHE_ELEM);
	RESET_STAT(QRM_CLONE_QSTR_ELEM);
	RESET_STAT(QRM_SQL_PARSER);
	RESET_STAT(QRM_AST_RESOLVE);
}

void 
cleanup_query_info(query_info_t *qinfo)
{
	FILE *f = fopen(STATS_FILE, "wb+");
	if (f) {
		print_stats(f, 1);
		fclose(f);
	}

	do_cleanup_query_info(qinfo);
	dest_stats();
}

void
set_user_session(query_info_t *qinfo, char *sessid, char *email, char *pwd)
{
	do_set_user_session(qinfo, sessid, email, pwd);
#if DEBUG
	char buf[512];
	memset(buf, 0, 512);
	sprintf(buf, "Log-in %s, pwd: %s, sessid: %s\n", email, pwd, sessid ? sessid : "NULL");
	dbg_log(qinfo, buf);
#endif
}

int 
run_refmon(query_info_t *qinfo, const char *text, size_t length, int is_utf8,
		int pe_method, char **rewritten_q, int *rewritten_qlen)
{

	char *select_str = (char *) "select";
	int select_str_len = strlen(select_str);

#if DEBUG
    printf("Running refmon! Length %d, strlen %d\n", length, select_str_len);
#endif

	if (length < select_str_len) 
		return -1;
	if (strncmp(text, select_str, select_str_len) != 0) {
#if DEBUG
        printf("text %s, select_str %s", text, select_str);
#endif
		return -1;
    }
#if CONFIG_REFMON == RM_NO_CACHE
	return run_refmon_no_cache(qinfo, text, length, is_utf8, pe_method,
			rewritten_q, rewritten_qlen);
#else
	return run_refmon_cache(qinfo, text, length, is_utf8, pe_method,
			rewritten_q, rewritten_qlen);
#endif
}

void
get_rewritten_query(query_info_t *qinfo, char **q, int *qlen)
{
	get_rewritten_query_from_query_info(qinfo, q, qlen);
}

// application always sends fully specified column names
void
set_expected_or_unexpected_columns(query_info_t *qinfo, char *cstr, int cstr_len, int expected)
{
	set_expected_or_unexpected_col_list(qinfo, cstr, cstr_len, expected);
}

void 
print_query(query_info_t *qinfo, const char *text, size_t length, int is_utf8)
{
	if (!qinfo || !text || !length)
		return;

#if DEBUG
	char *logstr = (char *) malloc(length + 64);
	memset(logstr, 0, length + 64);
	sprintf(logstr, "%s\n", text);
	log_query_info(qinfo, logstr);
	free(logstr);
#endif
}

void 
dbg_log(query_info_t *qinfo, char *logstr)
{
#if DEBUG
	log_query_info(qinfo, logstr);
#endif
}
