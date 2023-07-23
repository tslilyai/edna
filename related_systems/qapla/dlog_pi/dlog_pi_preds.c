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
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "dlog_pi.h"
#include "dlog_pi_helper.h"
#include "common/session.h"

uint8_t
dlog_pi_or(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx, dlog_pi_op_t *op,
		dlog_pi_env_t *env)
{
	return DLOG_PI_GRANTED_PERM;
}

uint8_t
dlog_pi_sessid(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx, 
		dlog_pi_op_t *op, dlog_pi_env_t *env)
{
	uint8_t cont_ret;
	uint8_t is_var_set = (lookup_elem_type(env, &op->tuple, 0) == TTYPE_VARIABLE);
	uint8_t check = DLOG_PI_SUCCESS;

	session_t *s = (session_t *) env->session;
	char *email = get_session_email(s);

	if (lookup_elem_type(env, &op->tuple, 0) != TTYPE_EMPTY)
		check |= compare_or_set(env, &op->tuple, 0, TTYPE_VARLEN, (void *) email,
				strlen(email)+1);

	if (check == DLOG_PI_SUCCESS) {
		cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
		if (cont_ret == DLOG_PI_GRANTED_PERM)
			return cont_ret;

		if (is_var_set)
			dlog_pi_env_unsetVar(env, *(uint16_t *) TUP_PTR_ELEMENT(&op->tuple, 0));
	}
	return DLOG_PI_DISALLOWED;
}

uint8_t
dlog_pi_time(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx, dlog_pi_op_t *op,
		dlog_pi_env_t *env)
{
	uint8_t cont_ret;
	uint8_t is_var_set = (lookup_elem_type(env, &op->tuple, 0) == TTYPE_VARIABLE);
	uint8_t check;

	uint64_t curr_time = time(NULL);
	check = compare_or_set(env, &op->tuple, 0, TTYPE_INTEGER, (void *) &curr_time, 
			sizeof(uint64_t));

	if (check == DLOG_PI_SUCCESS) {
		cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
		if (cont_ret == DLOG_PI_GRANTED_PERM)
			return cont_ret;

		if (is_var_set)
			dlog_pi_env_unsetVar(env, *(uint16_t *) TUP_PTR_ELEMENT(&op->tuple, 0));
	}
	return DLOG_PI_DISALLOWED;
}

uint8_t
dlog_pi_gt(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx, dlog_pi_op_t *op,
		dlog_pi_env_t *env)
{
	uint8_t lhs = 0, rhs = 1;
	uint8_t cont_ret = 0;
	tup_type_t lh_type = lookup_elem_type(env, &op->tuple, lhs);
	tup_type_t rh_type = lookup_elem_type(env, &op->tuple, rhs);
	if (lh_type == rh_type) {
		uint8_t *lh_value = lookup_elem_value(env, &op->tuple, lhs);
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint32_t lh_size = lookup_elem_size(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		uint8_t check = TUP_TYPE_GT(lh_type, lh_value, lh_size, rh_value, rh_size);
		if (check == DLOG_PI_SUCCESS) {
			cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
			if (cont_ret == DLOG_PI_GRANTED_PERM)
				return cont_ret;
		}
	} else if (lh_type == TTYPE_VARIABLE) {
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint16_t *varId = (uint16_t *) lookup_elem_value(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		dlog_pi_env_setVar(env, *varId, rh_type, rh_value, rh_size);
		cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
		if (cont_ret == DLOG_PI_GRANTED_PERM)
			return cont_ret;

		dlog_pi_env_unsetVar(env, *varId);
	}

	return DLOG_PI_DISALLOWED;
}

uint8_t
dlog_pi_ge(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx, dlog_pi_op_t *op,
		dlog_pi_env_t *env)
{
	uint8_t lhs = 0, rhs = 1;
	uint8_t cont_ret = 0;
	tup_type_t lh_type = lookup_elem_type(env, &op->tuple, lhs);
	tup_type_t rh_type = lookup_elem_type(env, &op->tuple, rhs);
	if (lh_type == rh_type) {
		uint8_t *lh_value = lookup_elem_value(env, &op->tuple, lhs);
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint32_t lh_size = lookup_elem_size(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		uint8_t ret;
		uint8_t check = DLOG_PI_FAILURE;
		ret = TUP_TYPE_GT(lh_type, lh_value, lh_size, rh_value, rh_size);
		if (ret == DLOG_PI_SUCCESS)
			check = ret;
		else {
			ret = TUP_TYPE_EQ(lh_type, lh_value, lh_size, rh_value, rh_size);
			if (ret == DLOG_PI_SUCCESS)
				check = ret;
		}

		if (check == DLOG_PI_SUCCESS) {
			cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
			if (cont_ret == DLOG_PI_GRANTED_PERM)
				return cont_ret;
		}
	} else if (lh_type == TTYPE_VARIABLE) {
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint16_t *varId = (uint16_t *) lookup_elem_value(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		dlog_pi_env_setVar(env, *varId, rh_type, rh_value, rh_size);
		cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
		if (cont_ret == DLOG_PI_GRANTED_PERM)
			return cont_ret;

		dlog_pi_env_unsetVar(env, *varId);
	}

	return DLOG_PI_DISALLOWED;
}

uint8_t
dlog_pi_lt(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx, dlog_pi_op_t *op,
		dlog_pi_env_t *env)
{
	uint8_t lhs = 0, rhs = 1;
	uint8_t cont_ret = 0;
	tup_type_t lh_type = lookup_elem_type(env, &op->tuple, lhs);
	tup_type_t rh_type = lookup_elem_type(env, &op->tuple, rhs);
	if (lh_type == rh_type) {
		uint8_t *lh_value = lookup_elem_value(env, &op->tuple, lhs);
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint32_t lh_size = lookup_elem_size(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		uint8_t check = TUP_TYPE_LT(lh_type, lh_value, lh_size, rh_value, rh_size);
		if (check == DLOG_PI_SUCCESS) {
			cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
			if (cont_ret == DLOG_PI_GRANTED_PERM)
				return cont_ret;
		}
	} else if (lh_type == TTYPE_VARIABLE) {
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint16_t *varId = (uint16_t *) lookup_elem_value(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		dlog_pi_env_setVar(env, *varId, rh_type, rh_value, rh_size);
		cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
		if (cont_ret == DLOG_PI_GRANTED_PERM)
			return cont_ret;

		dlog_pi_env_unsetVar(env, *varId);
	}

	return DLOG_PI_DISALLOWED;
}

uint8_t
dlog_pi_le(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx, dlog_pi_op_t *op,
		dlog_pi_env_t *env)
{
	uint8_t lhs = 0, rhs = 1;
	uint8_t cont_ret = 0;
	tup_type_t lh_type = lookup_elem_type(env, &op->tuple, lhs);
	tup_type_t rh_type = lookup_elem_type(env, &op->tuple, rhs);
	if (lh_type == rh_type) {
		uint8_t *lh_value = lookup_elem_value(env, &op->tuple, lhs);
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint32_t lh_size = lookup_elem_size(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		uint8_t ret;
		uint8_t check = DLOG_PI_FAILURE;
		ret = TUP_TYPE_GT(lh_type, lh_value, lh_size, rh_value, rh_size);
		if (ret == DLOG_PI_SUCCESS)
			check = ret;
		else {
			ret = TUP_TYPE_EQ(lh_type, lh_value, lh_size, rh_value, rh_size);
			if (ret == DLOG_PI_SUCCESS)
				check = ret;
		}

		if (check == DLOG_PI_SUCCESS) {
			cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
			if (cont_ret == DLOG_PI_GRANTED_PERM)
				return cont_ret;
		}
	} else if (lh_type == TTYPE_VARIABLE) {
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint16_t *varId = (uint16_t *) lookup_elem_value(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		dlog_pi_env_setVar(env, *varId, rh_type, rh_value, rh_size);
		cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
		if (cont_ret == DLOG_PI_GRANTED_PERM)
			return cont_ret;

		dlog_pi_env_unsetVar(env, *varId);
	}

	return DLOG_PI_DISALLOWED;
}

uint8_t
dlog_pi_eq(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx, dlog_pi_op_t *op,
		dlog_pi_env_t *env)
{
	uint8_t lhs = 0, rhs = 1;
	uint8_t cont_ret = 0;
	tup_type_t lh_type = lookup_elem_type(env, &op->tuple, lhs);
	tup_type_t rh_type = lookup_elem_type(env, &op->tuple, rhs);
	if (lh_type == rh_type) {
		uint8_t *lh_value = lookup_elem_value(env, &op->tuple, lhs);
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint32_t lh_size = lookup_elem_size(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		uint8_t check = TUP_TYPE_EQ(lh_type, lh_value, lh_size, rh_value, rh_size);
		if (check == DLOG_PI_SUCCESS) {
			cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
			if (cont_ret == DLOG_PI_GRANTED_PERM)
				return cont_ret;
		}
	} else if (lh_type == TTYPE_VARIABLE) {
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint16_t *varId = (uint16_t *) lookup_elem_value(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		dlog_pi_env_setVar(env, *varId, rh_type, rh_value, rh_size);
		cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
		if (cont_ret == DLOG_PI_GRANTED_PERM)
			return cont_ret;

		dlog_pi_env_unsetVar(env, *varId);
	}

	return DLOG_PI_DISALLOWED;
}

uint8_t
dlog_pi_ne(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx, dlog_pi_op_t *op,
		dlog_pi_env_t *env)
{
	uint8_t lhs = 0, rhs = 1;
	uint8_t cont_ret = 0;
	tup_type_t lh_type = lookup_elem_type(env, &op->tuple, lhs);
	tup_type_t rh_type = lookup_elem_type(env, &op->tuple, rhs);
	if (lh_type == rh_type) {
		uint8_t *lh_value = lookup_elem_value(env, &op->tuple, lhs);
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint32_t lh_size = lookup_elem_size(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		uint8_t check = TUP_TYPE_EQ(lh_type, lh_value, lh_size, rh_value, rh_size);
		if (check == DLOG_PI_FAILURE) {
			cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
			if (cont_ret == DLOG_PI_GRANTED_PERM)
				return cont_ret;
		}
	} else if (lh_type == TTYPE_VARIABLE) {
		uint8_t *rh_value = lookup_elem_value(env, &op->tuple, rhs);
		uint16_t *varId = (uint16_t *) lookup_elem_value(env, &op->tuple, lhs);
		uint32_t rh_size = lookup_elem_size(env, &op->tuple, rhs);
		dlog_pi_env_setVar(env, *varId, rh_type, rh_value, rh_size);
		cont_ret = dlog_pi_continue_eval(qp, perm, clause_idx, op, env);
		if (cont_ret == DLOG_PI_GRANTED_PERM)
			return cont_ret;

		dlog_pi_env_unsetVar(env, *varId);
	}

	return DLOG_PI_DISALLOWED;
}

dlog_pi_pred_fn dlog_pi_preds_fn[NUM_DLOG_P_CMDS] = {
	NULL,
	dlog_pi_or,
	dlog_pi_sessid,
	dlog_pi_time,
	dlog_pi_gt,
	dlog_pi_ge,
	dlog_pi_lt,
	dlog_pi_le,
	dlog_pi_eq,
	dlog_pi_ne,
};
