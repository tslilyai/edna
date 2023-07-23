/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QUERY_CACHE_H__
#define __QUERY_CACHE_H__

#ifdef __cplusplus
extern "C" {
#endif

#include "utils/hashtable.h"

	// h(q), time/valid -> rw_q
	typedef hash_table_t query_cache_t;

	typedef struct qcache_elem {
		char *orig_q;
		int orig_qlen;
		uint64_t expiry; // -1 if always valid
		list_t rw_q_list;
		list_t table_listp;
	} qcache_elem_t;

	extern hash_fn_t qcache_ht_func;

	void init_query_cache(query_cache_t *qcache, int size);
	void reset_query_cache(query_cache_t *qcache);
	void cleanup_query_cache(query_cache_t *qcache);
	void query_cache_add_entry(query_cache_t *qcache, qcache_elem_t *qce);
	void *query_cache_get_entry(query_cache_t *qcache, void *key, 
			int keylen, int exact);
	void print_query_cache(query_cache_t *qcache);

	qcache_elem_t *init_qcache_elem(char *orig_q, int orig_qlen, list_t *rw_q_list);
	void free_qcache_elem(qcache_elem_t **qce);
	void set_qcache_elem_key(qcache_elem_t *qce, char *orig_q, int orig_qlen);
	void set_qcache_elem_val(qcache_elem_t *qce, list_t *rw_q_list);
	void print_qcache_elem(qcache_elem_t *qce);
#ifdef __cplusplus
}
#endif

#endif /* __QUERY_CACHE_H__ */
