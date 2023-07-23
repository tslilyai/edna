/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __POL_EVAL_COL_H__
#define __POL_EVAL_COL_H__

#include "utils/list.h"
#include "common/qapla_policy.h"
#include "query_info_int.h"

#ifdef __cplusplus
extern "C" {
#endif

	int query_get_pid_for_each_col(query_info_int_t *qi, list_t *table_list,
			qapla_policy_t **qp_list, int *n_qp);
	int query_get_sql_for_table(query_info_int_t *qi, list_t *table_list, 
			char **col_sql, int *col_sql_len, char ***tab_sql_str, int **tab_sql_str_len,
			int *n_tab_sql);

#ifdef __cplusplus
}
#endif

#endif /* __POL_EVAL_COL_H__ */
