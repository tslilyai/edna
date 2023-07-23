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
#include <stdint.h>
#include <string.h>

#include "common/tuple.h"
#include "dlog_pi_env.h"

uint8_t 
dlog_pi_env_init(dlog_pi_env_t *env, qapla_policy_t *qp,
		qapla_perm_id_t perm, void *session)
{
	uint16_t var_count = qapla_get_num_perm_vars(qp, perm);
	memset(env, 0, sizeof(dlog_pi_env_t));

	if (var_count > 0) {
		env->vars = (qapla_var_int_t *)calloc(var_count, sizeof(qapla_var_int_t));
		if (!env->vars)
			return PI_ENV_OOM;

		memset(env->vars, 0, var_count * sizeof(qapla_var_int_t));
	}

	env->var_count = var_count;
	env->session = session;
	return PI_ENV_SUCCESS;
}

uint8_t 
dlog_pi_env_cleanup(dlog_pi_env_t *env)
{
	dlog_pi_env_unsetAllVars(env);
	if (env->vars)
		free(env->vars);

	memset(env, 0, sizeof(dlog_pi_env_t));
	return PI_ENV_SUCCESS;
}

uint8_t 
dlog_pi_env_setVar(dlog_pi_env_t *env, uint8_t varId, uint8_t type,
		uint8_t *val, uint32_t val_len)
{
	if (varId >= env->var_count)
		return PI_ENV_INVALID;

	uint8_t *ptr;
	uint16_t off;
	TUP_ELEMENT_TYPE_SET(env->vars[varId].type, 0, type);
	if (type == TTYPE_VARLEN) {
		ptr = (uint8_t *) malloc(val_len+8);
		memset(ptr, 0, val_len+8);
		TUP_ELEMENT_TYPE_SET(*((dlog_tuple_t *) ptr), 0, type);
		off = SIZEOF_TUP(*((dlog_tuple_t *) ptr));
		uint16_t varr[2] = {off, off + val_len};
		void *elem_start = TUP_PTR_ELEMENT((dlog_tuple_t *) ptr, 0);
		memcpy(elem_start, (void *) varr, TUP_TYPE_SIZE(type));

		memcpy((char *) ptr + off, (char *) val, val_len-1);
		env->vars[varId].len = val_len;
		env->vars[varId].content2 = ptr;
	} else {
		ptr = env->vars[varId].content;
		memset(ptr, 0, MAX_VAR_CONTENT_LEN);
		memcpy(ptr, val, val_len);
	}

	return PI_ENV_SUCCESS;
}

uint8_t
dlog_pi_env_getVar(dlog_pi_env_t *env, uint8_t varId, uint8_t **t)
{
	if (varId >= env->var_count)
		return PI_ENV_INVALID;

	if (env->vars[varId].type == TTYPE_EMPTY)
		return PI_ENV_VAR_NOT_SET;

	if (env->vars[varId].type != TTYPE_VARLEN)
		*t = (uint8_t *) &(env->vars[varId]);
	else {
		*t = (uint8_t *) env->vars[varId].content2;
	}
	return PI_ENV_SUCCESS;
}

uint8_t
dlog_pi_env_unsetVar(dlog_pi_env_t *env, uint8_t varId)
{
	if (env->vars[varId].type == TTYPE_EMPTY)
		return PI_ENV_VAR_NOT_SET;

	if (env->vars[varId].type == TTYPE_VARLEN) {
		if (env->vars[varId].content2)
			free(env->vars[varId].content2);
	}

	env->vars[varId].type = TTYPE_EMPTY;
	// should zero out the tuple as well
	return PI_ENV_SUCCESS;
}

uint8_t
dlog_pi_env_unsetAllVars(dlog_pi_env_t *env)
{
	uint8_t varIt = 0;
	uint8_t ret = PI_ENV_SUCCESS;
	for (varIt = 0; ret == PI_ENV_SUCCESS && varIt < env->var_count; varIt++) {
		ret |= dlog_pi_env_unsetVar(env, varIt);
	}

	return ret;
}
