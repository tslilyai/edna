/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#include <stdio.h>
#include "common/qapla_policy.h"
#include "common/dlog_pi_tools.h"
#include "dlog_pi.h"

static uint8_t
dlog_pi_next_or_exists(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx,
		dlog_pi_op_t ** currpos)
{
	dlog_pi_op_t *next = *currpos;
	char *perm_end = qapla_get_perm_clause_end(qp, perm, CTYPE_DLOG, clause_idx);
	do {
		next = dlog_pi_next_op(next);
	} while (next < (dlog_pi_op_t *) perm_end && next->cmd != DLOG_P_OR);

	if (next && next < (dlog_pi_op_t *) perm_end && next->cmd == DLOG_P_OR) {
		*currpos = dlog_pi_next_op(next);
		return DLOG_PI_SUCCESS;
	}

	return DLOG_PI_FAILURE;
}

// caller must unset env variables in case policy eval succeeds
uint8_t
dlog_pi_evaluate(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx, 
		dlog_pi_env_t *env)
{
	if (!qp || perm >= QP_NUM_PERMS || !env) {
		return DLOG_PI_DISALLOWED;
	}

	// if any of the OR clauses is set to allowed, return immediately
	if (check_allow_all_clause(qp, perm, CTYPE_DLOG, clause_idx))
		return DLOG_PI_GRANTED_PERM;

	dlog_pi_op_t *currpos = 
		(dlog_pi_op_t *) qapla_get_perm_clause_start(qp, perm, CTYPE_DLOG, clause_idx);
	uint32_t cnt_or = 0;
	uint8_t ret = 0;
	do {
		if (currpos->cmd == DLOG_P_EMPTY)
			return DLOG_PI_DISALLOWED;

		ret = DLOG_P_INVOKE_CMD(qp, perm, clause_idx, currpos, env);
		if (ret == DLOG_PI_GRANTED_PERM)
			return ret;

		dlog_pi_env_unsetAllVars(env);
		cnt_or++;

	} while (dlog_pi_next_or_exists(qp, perm, clause_idx, &currpos) == DLOG_PI_SUCCESS);

	return DLOG_PI_DISALLOWED;
}

uint8_t
dlog_pi_continue_eval(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx,
		dlog_pi_op_t *curr_op, dlog_pi_env_t *env)
{
	dlog_pi_op_t *next_op = dlog_pi_next_op(curr_op);
	dlog_pi_op_t *end = 
		(dlog_pi_op_t *) qapla_get_perm_clause_end(qp, perm, CTYPE_DLOG, clause_idx);

	if (next_op >= end || (next_op->cmd == DLOG_P_OR || next_op->cmd == DLOG_P_EMPTY))
		return DLOG_PI_GRANTED_PERM;

	uint8_t ret = 0;
	ret = DLOG_P_INVOKE_CMD(qp, perm, clause_idx, next_op, env);
	return ret;
}

dlog_pi_op_t *
dlog_pi_next_op(dlog_pi_op_t *curr_op)
{
	return next_operation(curr_op);
}
