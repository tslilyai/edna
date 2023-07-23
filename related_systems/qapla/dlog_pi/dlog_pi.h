/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __DLOG_PI_H__
#define __DLOG_PI_H__

#include "dlog_pi_env.h"
#include "common/dlog_pi_ds.h"
#include "common/qapla_policy.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DLOG_PI_SUCCESS 0
#define DLOG_PI_FAILURE 1
#define DLOG_PI_NOT_IMPLEMENTED 2
#define DLOG_PI_INVALID_CMD 3
#define DLOG_PI_GRANTED_PERM 4
#define DLOG_PI_DISALLOWED 5

#define DLOG_P_INVOKE_CMD(pol, perm, cidx, op, env)	\
	dlog_pi_preds_fn[op->cmd](pol, perm, cidx, op, env)

typedef uint8_t (*dlog_pi_pred_fn)(qapla_policy_t *qp, qapla_perm_id_t perm,
		int clause_idx, dlog_pi_op_t *op, dlog_pi_env_t *env);

extern dlog_pi_pred_fn dlog_pi_preds_fn[NUM_DLOG_P_CMDS];

uint8_t dlog_pi_evaluate(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx,
		dlog_pi_env_t *env);

uint8_t dlog_pi_continue_eval(qapla_policy_t *qp, qapla_perm_id_t perm, 
		int clause_idx, dlog_pi_op_t *curr_op, dlog_pi_env_t *env);

dlog_pi_op_t * dlog_pi_next_op(dlog_pi_op_t *curr_op);

#ifdef __cplusplus
}
#endif

#endif /* __DLOG_PI_H__ */
