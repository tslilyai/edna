/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __POL_VECTOR_H__
#define __POL_VECTOR_H__

#include "utils/list.h"
#include "utils/hashtable.h"
#include "common/db.h"
#include "common/col_sym.h"

#ifdef __cplusplus
extern "C" {
#endif

	typedef uint64_t PVEC;

	typedef struct pvec_fn {
		hash_table_t * (*init_pol_vector)(int size);
		hash_table_t * (*generate_schema_pol_vector)(db_t *schema);
		void (*free_schema_pol_vector)(hash_table_t **pol_vec_index);
		void (*print_schema_pol_vector)(hash_table_t *pol_vec_index, FILE *f);
		void (*add_policy_to_col_list)(hash_table_t *pol_vec_index, int pid, list_t *cs_list);
		void (*add_policy_to_col)(hash_table_t *pol_vec_index, int pid, col_sym_t *cs);
		PVEC (*compute_query_pol_vector)(hash_table_t *pol_vec_index, list_t *cs_list);
		PVEC (*get_col_pol_vector)(hash_table_t *pol_vec_index, col_sym_t *cs);
	} pvec_fn_t;

	extern pvec_fn_t ht_colname_pvec;
	extern pvec_fn_t ht_colid_pvec;
	
	typedef struct pvec {
		void *priv;
	} pvec_t;

	int16_t get_pid_from_pvec(PVEC pv);

	void init_pvec_index(pvec_t *pv, pvec_fn_t *pfn, int index_size);
	void init_schema_pvec(pvec_t *pv, pvec_fn_t *pfn, db_t *schema);
	pvec_t *alloc_init_pvec_index(pvec_fn_t *pfn, db_t *schema);
	void cleanup_pvec_index(pvec_t *pv);
	void free_pvec_index(pvec_t **pv);
	void add_policy_to_pvec_index(pvec_t *pv, uint16_t pid, list_t *clist);
	void add_col_policy_to_pvec_index(pvec_t *pv, uint16_t pid, col_sym_t *cs);
	PVEC get_policy_from_pvec_index(pvec_t *pv, list_t *clist);
	PVEC get_column_pvec(pvec_t *pv, col_sym_t *cs);
	void print_pvec_index(pvec_t *pv, FILE *f);

	PVEC get_query_pol_history();
	void update_query_pol_history(PVEC polvec);

#ifdef __cplusplus
}
#endif

#endif /* __POL_VECTOR_H__ */
