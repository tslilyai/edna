/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */

/*
 * functions to create SQL policy for queries
 * executed on materialized views of original DB
 */

#ifndef __POL_EVAL_MV_H__
#define __POL_EVAL_MV_H__

#include "query_info_int.h"
#include "query_symbol.h"

#ifdef __cplusplus
extern "C" {
#endif

	int query_get_table_list(query_info_int_t *qi, uint64_t *tid_arr, int max_tab, int *n_tab);
	int query_segregate_cols_to_tables(query_info_int_t *qi, list_t *table_list);
	int get_mv_sql_for_table(query_info_int_t *qi, symbol_t *tsym, char **sql, int *sql_len);

#ifdef __cplusplus
}
#endif

#endif /* __POL_EVAL_MV_H__ */
