/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QUERY_INFO_H__
#define __QUERY_INFO_H__

#include "metadata.h"

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct query_info {
		void *priv;
	} query_info_t;

	void _do_init_query_info(query_info_t *qinfo, const char *schema_name,
			const char *logname, int db_type, app_info_t *ai);
	int qinfo_get_dbtype(query_info_t *qinfo);
	void do_set_user_session(query_info_t *qinfo, char *sessid, char *email, char *pwd);
	void set_expected_or_unexpected_col_list(query_info_t *qinfo, char *cstr,
			int cstr_len, int expected);
	int do_eval_query_policies(query_info_t *qinfo);
	void setup_query_in_query_info(query_info_t *qinfo, const char *text,
			size_t length, int pe_method);
	void get_rewritten_query_from_query_info(query_info_t *qinfo, char **q, int *qlen);
	void do_reset_query_info(query_info_t *qinfo);
	void do_reset_query_cache(query_info_t *qinfo);
	void do_cleanup_query_info(query_info_t *qinfo);
	void log_query_info(query_info_t *qinfo, char *logstr);
	void print_query_info(query_info_t *qinfo);

	int query_parser(query_info_t *qinfo, const char *text, size_t length,
			int is_utf8, int pe_method);
	int run_refmon_no_cache(query_info_t *qinfo, const char *text, size_t length,
			int is_utf8, int pe_method, char **rewritten_q, int *rewritten_qlen);
	int run_refmon_cache(query_info_t *qinfo, const char *text, size_t length,
			int is_utf8, int pe_method, char **rewritten_q, int *rewritten_qlen);

	void get_query_symbols(void *qip, void *ast);
#ifdef __cplusplus
}
#endif

#endif /* __QUERY_INFO_H__ */
