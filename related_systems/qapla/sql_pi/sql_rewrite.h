/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __SQL_REWRITE_H__
#define __SQL_REWRITE_H__

#include <stdint.h>
#include "common/query.h"
#include "qinfo/query_info_int.h"

#ifdef __cplusplus
extern "C" {
#endif

	uint8_t sql_rewrite(query_info_int_t *qi, list_t *table_list, char **tab_sql, 
			int *tab_sql_len, int n_tab_sql);
	int compute_frag_len(char *orig_q, qpos_t *curr, qpos_t *prev);
	void copy_frag(char *new_q, qpos_t *curr, qpos_t *prev, char *orig_q, int len);

	void resolve_policy_parameters(query_info_int_t *qi, list_t *qsplit_list);

#ifdef __cplusplus
}
#endif

#endif /* __SQL_REWRITE_H__ */
