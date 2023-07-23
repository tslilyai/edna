/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __SQL_POL_EVAL_H__
#define __SQL_POL_EVAL_H__

#include "utils/list.h"
#include "common/qapla_policy.h"
#include "dlog_pi/dlog_pi_env.h"

#define SQL_PI_SUCCESS 0
#define SQL_PI_FAILURE 1
#define SQL_PI_NOT_IMPLEMENTED 2
#define SQL_PI_INVALID_PLC 3
#define SQL_PI_GRANTED_PERM 4
#define SQL_PI_DISALLOWED 5

#ifdef __cplusplus
extern "C" {
#endif
	
	uint8_t sql_parse_params(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx,
		dlog_pi_env_t *env, qapla_policy_t *out_qp, list_t *tlist);
	int match_sql_table_in_pol(qapla_policy_t *qp, qapla_perm_id_t perm, uint64_t tid);

	void cleanup_placeholder_list(list_t *plc_list);
	int get_policy_parameters(char *q, int qlen, list_t *plc_list);
	void fill_policy_parameters(char *q, int qlen, list_t *plc_list, char **new_q, int *new_qlen, char *email, dlog_pi_env_t *env);

#ifdef __cplusplus
}
#endif

#endif /* __SQL_POL_EVAL_H__ */
