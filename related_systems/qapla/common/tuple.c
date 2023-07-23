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
#include <stdint.h>
#include <string.h>
#include "tuple.h"
#include "types.h"

uint8_t tup_type_size[NUM_TUP_TYPES] = {0, 8, 2, 4, sizeof(uint32_t)};
const char *tup_type_name[NUM_TUP_TYPES] = {
	"empty", 
	"integer", 
	"variable",
	"tuple", 
	"varlen"
};

uint32_t
TUP_ELEMENT_START(dlog_tuple_t *tuple, int i)
{
	dlog_tuple_t t = *tuple;
	uint32_t sum = 0;
	uint32_t type_size = 0;

	for (i = i - 1; i > -1; i--) {
		if (TUP_ELEMENT_TYPE(t, i) != TTYPE_TUPLE)
			type_size = TUP_ELEMENT_TYPE_SIZE(t, i);
		else {
			dlog_tuple_t *tmp = (dlog_tuple_t *) TUP_PTR_ELEMENT(tuple, i);
			type_size = sizeof(dlog_tuple_t) + TUP_ELEMENT_START(tmp, TUP_MAX_NUM_ARGS);
		}
		sum += type_size;
	}
	return sum;
}

uint32_t 
TUP_ELEMENT_START_VAR_LEN(dlog_tuple_t *tuple, int i)
{
	dlog_tuple_t t = *tuple;
	uint32_t sum = 0, type;
	uint32_t type_size = 0;
	uint32_t max_var_len = 0;

	for (i = i - 1; i > -1; i--) {
		type = TUP_ELEMENT_TYPE(t, i);
		if (type == TTYPE_VARLEN) {
			uint16_t *varr = (uint16_t *) TUP_PTR_ELEMENT(tuple, i);
			max_var_len += (varr[1] - varr[0]);
			//if (max_var_len < varr[1])
			//	max_var_len = varr[1] - sizeof(dlog_tuple_t);
			type_size = TUP_ELEMENT_TYPE_SIZE(t, i);
		} else if (type == TTYPE_TUPLE) {
			dlog_tuple_t *tmp = (dlog_tuple_t *) TUP_PTR_ELEMENT(tuple, i);
			type_size = sizeof(dlog_tuple_t) + TUP_ELEMENT_START_VAR_LEN(tmp, TUP_MAX_NUM_ARGS);
		} else {
			type_size = TUP_ELEMENT_TYPE_SIZE(t, i);
		}
		sum += type_size;
	}

	uint32_t max = sum + max_var_len;
	//uint32_t max = (sum > max_var_len) ? sum : max_var_len;
	return max;
}

char *
get_var_length_ptr(dlog_tuple_t *tup, int i)
{
	uint16_t *varr = (uint16_t *) TUP_PTR_ELEMENT(tup, i);
	return (char *) ((char *) tup + varr[0]);
}

uint32_t
get_var_length_ptr_len(dlog_tuple_t *tup, int i)
{
	uint16_t *varr = (uint16_t *) TUP_PTR_ELEMENT(tup, i);
	return (uint32_t) (varr[1] - varr[0]);
}

void
print_binary_to_char(char *str, int str_len, char *buf)
{
	uint32_t it = 0;
	for (it = 0; it < HASH_BUF_LEN; it++) {
		sprintf(buf + (2 * it), "%02X", *(unsigned char *) (str + it));
	}
}

void
print_tuple(dlog_tuple_t *tup, char *buf)
{
	char tmp[4096];
	memset(tmp, 0, 4096);
	uint32_t arg_it = 0, offset = 0;
	for (arg_it = 0; arg_it < TUP_MAX_NUM_ARGS; ++arg_it) {
		uint32_t type = TUP_ELEMENT_TYPE(*tup, arg_it);
		sprintf(tmp, " %d: %d -> ", arg_it, type);
		uint32_t len = strlen(tmp);
		uint8_t *value = (uint8_t *) TUP_PTR_ELEMENT(tup, arg_it);
		switch (type) {
			case TTYPE_EMPTY:
				sprintf(tmp + len, "NULL");
				break;
			case TTYPE_INTEGER:
				sprintf(tmp + len, "%lld", (unsigned long long) *((uint64_t *) value));
				break;
			case TTYPE_VARIABLE:
				sprintf(tmp + len, "%d", *((unsigned short *) value));
				break;
			case TTYPE_TUPLE:
				print_tuple((dlog_tuple_t *) value, tmp + len);
				break;
			case TTYPE_VARLEN:
				{
					uint16_t *varr = (uint16_t *) value;
					sprintf(tmp + len, "[%d, %d] ", varr[0], varr[1]);
					len = strlen(tmp);
					snprintf(tmp + len, (varr[1] - varr[0]), "%s", (char *) tup + varr[0]);
					len = strlen(tmp);
				}
				break;
		}

		if (type == TTYPE_EMPTY) {
			sprintf(buf + offset, "%s", tmp);
			len = strlen(tmp);
			offset += len;
			break;
		} else {
			sprintf(buf + offset, "%s;", tmp);
			len = strlen(tmp);
			offset += len+1;
		}
	}
}

uint8_t type_not_implemented()
{
	return TUP_NOT_IMPLEMENTED;
}

tup_type_if_t type_if[NUM_TUP_TYPES] = {
	// empty
	{
		(tup_eq_fn) type_not_implemented,
		(tup_gt_fn) type_not_implemented,
		(tup_ge_fn) type_not_implemented,
		(tup_lt_fn) type_not_implemented,
		(tup_le_fn) type_not_implemented
	},
	// integer,
	{
		type_int_eq,
		type_int_gt,
		type_int_ge,
		type_int_lt,
		type_int_le
	},
	// variable
	{
		(tup_eq_fn) type_not_implemented,
		(tup_gt_fn) type_not_implemented,
		(tup_ge_fn) type_not_implemented,
		(tup_lt_fn) type_not_implemented,
		(tup_le_fn) type_not_implemented
	},
	// tuple
	{
		(tup_eq_fn) type_not_implemented,
		(tup_gt_fn) type_not_implemented,
		(tup_ge_fn) type_not_implemented,
		(tup_lt_fn) type_not_implemented,
		(tup_le_fn) type_not_implemented
	},
	// varlen
	{
		type_varlen_eq,
		type_varlen_gt,
		type_varlen_ge,
		type_varlen_lt,
		type_varlen_le
	}
};
