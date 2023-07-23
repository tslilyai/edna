/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __POL_EVAL_H__
#define __POL_EVAL_H__

#include "common/qapla_policy.h"

#include "query_info_int.h"
#include "pol_vector.h"
#include "pol_cluster.h"

#define POL_EVAL_SUCCESS	0
#define POL_EVAL_ERROR	(-1)

#ifdef __cplusplus
extern "C" {
#endif

	int aggr_query_get_pvec(query_info_int_t *qi, PVEC *pv);
	int query_get_pvec(query_info_int_t *qi, PVEC *pv);
	int query_pvec_get_policy_list(query_info_int_t *qi, PVEC pv, 
			qapla_policy_t ***pol_list, int *n_pol_list);
	int get_sql_clauses_in_pol(qapla_policy_t *qp, query_info_int_t *qi, uint64_t tid,
			char **q, int *qlen);
	int get_resolved_sql_pol(query_info_int_t *qi, qapla_policy_t **qp, int n_qp,
			qapla_perm_id_t perm, char ***tab_sql_str, int **tab_sql_str_len, 
			int *n_tab_sql, list_t *tab_list);
	int free_policy_list(qapla_policy_t ***pol_list, int n_pol_list);
	
	void error_print_col_link(FILE *f, col_sym_t *cs);

#ifdef __cplusplus
}
#endif

#endif /* __POL_EVAL_H__ */
