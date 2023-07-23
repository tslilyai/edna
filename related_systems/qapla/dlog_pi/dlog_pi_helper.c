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
#include "dlog_pi_helper.h"
#include "dlog_pi_env.h"
#include "dlog_pi.h"
#include "common/tuple.h"

tup_type_t 
lookup_elem_type(dlog_pi_env_t *env, dlog_tuple_t *tup, uint8_t id)
{
	if (TUP_ELEMENT_TYPE(*tup, id) != TTYPE_VARIABLE)
		return (tup_type_t) TUP_ELEMENT_TYPE(*tup, id);

	uint16_t varId = *(uint16_t *) TUP_PTR_ELEMENT(tup, id);
	dlog_tuple_t *ret;
	if (dlog_pi_env_getVar(env, varId, (uint8_t **) &ret) == PI_ENV_VAR_NOT_SET)
		return TTYPE_VARIABLE;

	return (tup_type_t) TUP_ELEMENT_TYPE(*ret, 0);
}

uint8_t *
lookup_elem_value(dlog_pi_env_t *env, dlog_tuple_t *tup, uint8_t id)
{
	if (TUP_ELEMENT_TYPE(*tup, id) == TTYPE_VARIABLE) {
		uint16_t varId = *(uint16_t *) TUP_PTR_ELEMENT(tup, id);
		dlog_tuple_t *ret;
		if (dlog_pi_env_getVar(env, varId, (uint8_t **) &ret) == PI_ENV_VAR_NOT_SET)
			return (uint8_t *) TUP_PTR_ELEMENT(tup, id);

		if (TUP_ELEMENT_TYPE(*ret, 0) == TTYPE_VARLEN) {
			uint8_t *retvar = (uint8_t *) get_var_length_ptr(ret, 0);
			return retvar;
		}

		return (uint8_t *) TUP_PTR_ELEMENT(ret, 0);
	} else if (TUP_ELEMENT_TYPE(*tup, id) == TTYPE_VARLEN) {
		return (uint8_t *) get_var_length_ptr(tup, id);
		//uint16_t *varr = (uint16_t *) TUP_PTR_ELEMENT(tup, id);
		//printf("varlen: %d, %d\n", varr[0], varr[1]);
		//return lookup_var_len_ptr(env, tup, id);
	}

	return (uint8_t *) TUP_PTR_ELEMENT(tup, id);
}

uint32_t
lookup_elem_size(dlog_pi_env_t *env, dlog_tuple_t *tup, uint8_t id)
{
	if (TUP_ELEMENT_TYPE(*tup, id) == TTYPE_VARIABLE) {
		uint16_t varId = *(uint16_t *) TUP_PTR_ELEMENT(tup, id);
		dlog_tuple_t *ret;
		if (dlog_pi_env_getVar(env, varId, (uint8_t **) &ret) == PI_ENV_VAR_NOT_SET)
			return TUP_ELEMENT_TYPE_SIZE(*tup, id);

		if (TUP_ELEMENT_TYPE(*ret, 0) == TTYPE_VARLEN) {
			return get_var_length_ptr_len(ret, 0);
			//uint16_t *varr = (uint16_t *) TUP_PTR_ELEMENT(*ret, 0);
			//return (uint32_t) (varr[1] - varr[0]);
		}
		return TUP_ELEMENT_TYPE_SIZE(*ret, 0);
	} else if (TUP_ELEMENT_TYPE(*tup, id) != TTYPE_VARLEN) {
		return TUP_ELEMENT_TYPE_SIZE(*tup, id);
	} else {
		return get_var_length_ptr_len(tup, id);
		//printf("varlen size: %d\n", TUP_ELEMENT_TYPE_SIZE(*tup, id));
		//uint16_t *varr = (uint16_t *) TUP_PTR_ELEMENT(tup, id);
		//return (uint32_t) (varr[1] - varr[0]);
	}
}

uint8_t *
lookup_var_len_ptr(dlog_pi_env_t *env, dlog_tuple_t *tup, uint8_t id)
{
	if (TUP_ELEMENT_TYPE(*tup, id) == TTYPE_VARIABLE) {
		uint16_t varId = *(uint16_t *) TUP_PTR_ELEMENT(tup, id);
		uint8_t *retval = NULL;
		dlog_tuple_t *ret;
		if (dlog_pi_env_getVar(env, varId, (uint8_t **) &ret) == PI_ENV_VAR_NOT_SET)
			return NULL;

		retval = (uint8_t *) TUP_PTR_ELEMENT(ret, 0);
		return retval;
	}

	if (TUP_ELEMENT_TYPE(*tup, id) == TTYPE_VARLEN) {
		return (uint8_t *) get_var_length_ptr(tup, id);
	}

	return NULL;
}

uint8_t 
compare_or_set(dlog_pi_env_t *env, dlog_tuple_t *tup, uint8_t id,
		tup_type_t type, void *env_content, int env_cont_len)
{
	dlog_tuple_t lh_type = lookup_elem_type(env, tup, id);
	if (lh_type == TTYPE_VARIABLE) {
		uint16_t varId = *(uint16_t *) lookup_elem_value(env, tup, id);
		dlog_pi_env_setVar(env, varId, type, (uint8_t *) env_content, env_cont_len);
		return DLOG_PI_SUCCESS;
	} else {
		if (lh_type != type)
			return DLOG_PI_NOT_IMPLEMENTED;

		uint8_t *lh_value = (uint8_t *) lookup_elem_value(env, tup, id);
		uint32_t lh_len = lookup_elem_size(env, tup, id);
		if (TUP_TYPE_EQ(lh_type, lh_value, lh_len, 
					(uint8_t *) env_content, env_cont_len) == DLOG_PI_SUCCESS)
			return DLOG_PI_SUCCESS;

		return DLOG_PI_DISALLOWED;
	}
}
