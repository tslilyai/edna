/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QUERY_INFO_INT_H__
#define __QUERY_INFO_INT_H__

#include "utils/list.h"
#include "utils/statistics.h"
#include "utils/stats_defs.h"

#include "common/db.h"
#include "common/sym.h"
#include "common/query.h"
#include "common/session.h"

#include "query_context.h"
#include "query_cache.h"
#include "metadata.h"


#ifdef __cplusplus
extern "C" {
#endif

	STATS_DEFS_MAP(DECL_STAT_EXTERN);

	typedef struct log_info {
		char *fname;
		FILE *f;
	} log_info_t;

	typedef struct query_info_int {
		db_t schema;
		log_info_t log_info;
		metadata_t qm;	// qapla policy metadata
		qstr_t qstr;
		parser_context_t pc;
		session_t session;
		query_cache_t qcache;
		int db_type;
		void *conn;
		void *parser;
	} query_info_int_t;

	int print_context(void *qi, void *data);
	int reset_context_visited(void *qi, void *data);

	typedef int (*do_func)(void *qi, void *data);
	void traverse_context_list(void *qi, do_func fn);
	void traverse_context_list_recursive(void *qi, do_func fn);

#ifdef __cplusplus
}
#endif

#endif /* __QUERY_INFO_INT_H__ */
