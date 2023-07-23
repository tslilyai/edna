/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QAPLA_PARSER_IF_H__
#define __QAPLA_PARSER_IF_H__

#include "qinfo/query_info.h"
#include "qinfo/metadata.h"

#ifdef __cplusplus
extern "C" {
#endif

	void init_query_info(query_info_t *qinfo, const char *schema_name,
			const char *logname, app_info_t *ai);
	void _init_query_info(query_info_t *qinfo, const char *schema_name,
			const char *logname, int db_type, app_info_t *ai);
	void set_user_session(query_info_t *qinfo, char *sessid, char *email, char *pwd);
	void set_expected_or_unexpected_columns(query_info_t *qinfo,
			char *cstr, int cstr_len, int expected);
	int run_refmon(query_info_t *qinfo, const char *text, size_t length,
			int is_utf8, int pe_method, char **rewritten_q, int *rewritten_qlen);
	void get_rewritten_query(query_info_t *qinfo, char **q, int *qlen);
	void print_query(query_info_t *qinfo, const char *text, size_t length, int is_utf8);
	void reset_query_info(query_info_t *qinfo);
	void flush_query_info_cache(query_info_t *qinfo);
	void cleanup_query_info(query_info_t *qinfo);
	void dbg_log(query_info_t *qinfo, char *logstr);

#ifdef __cplusplus
}
#endif

#endif /* __QAPLA_PARSER_IF_H__ */
