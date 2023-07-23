/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __POL_EVAL_UTILS_H__
#define __POL_EVAL_UTILS_H__

#include "utils/list.h"

#ifdef __cplusplus
extern "C" {
#endif

	int get_distinct_table_array(uint64_t *tab_arr, int *num_tab, int max_tab, 
			list_t *t_list);
	int get_table_index(uint64_t *tab_arr, int num_tab, uint64_t tid);
	symbol_t *get_table_sym(list_t *tab_list, uint64_t tid);
	void print_table_col_list(list_t *tab_list, FILE *f);

#ifdef __cplusplus
}
#endif

#endif /* __POL_EVAL_UTILS_H__ */
