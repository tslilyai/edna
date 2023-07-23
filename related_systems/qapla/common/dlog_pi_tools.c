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
#include <stdarg.h>
#include <string.h>

#include "dlog_pi_tools.h"
#include "dlog_pi_ds.h"

const char *dlog_perm_names[NUM_DLOG_P_CMDS] = {
	"empty",
	"OR",
	"session ID",
	"time",
	"GT",
	"GE",
	"LT",
	"LE",
	"EQ",
	"NE",
	"SQL cmd"
};

dlog_pi_op_t *
create_n_arg_cmd(dlog_pi_op_t *op, dlog_pol_cmd_t cmd, uint32_t num_args, ...)
{
	uint32_t it = 0, type;
	va_list ap;

	op->cmd = cmd;
	op->tuple = 0;
	char *tuple_off;

	va_start(ap, num_args);
	for (it = 0; it < num_args; it++) {
		type = va_arg(ap, uint32_t);
		(void) va_arg(ap, uint16_t *);
		if (type == TTYPE_VARLEN)
			(void) va_arg(ap, uint16_t *);

		TUP_ELEMENT_TYPE_SET(op->tuple, it, type);
		tuple_off = (char *) TUP_PTR_ELEMENT(&op->tuple, it);
		memset(tuple_off, 0, tup_type_size[type]);
	}
	va_end(ap);

	uint16_t tup_size = SIZEOF_TUP(op->tuple);
	uint16_t var_len_added = 0;

	va_start(ap, num_args);
	for (it = 0; it < num_args; it++) {
		type = va_arg(ap, uint32_t);
		uint16_t *value = va_arg(ap, uint16_t *);

		if (type == TTYPE_EMPTY)
			continue;

		if (type == TTYPE_TUPLE) {
		} else if (type == TTYPE_VARLEN) {
			uint16_t *start_var_len = value;
			uint16_t *end_var_len = va_arg(ap, uint16_t *);
			char *tup_start = (char *) &op->tuple;
			uint16_t start = tup_size + var_len_added;
			uint16_t length = ((char *) end_var_len) - ((char *) start_var_len);
			uint16_t var_len[2] = {start, start + length};
			void *elem_start = TUP_ELEMENT(op->tuple, it);
			memcpy(elem_start, var_len, TUP_TYPE_SIZE(type));

			char *start_addr = tup_start + start;
			memcpy(start_addr, start_var_len, length);

			var_len_added += length;
		} else {
			void *elem_start = TUP_ELEMENT(op->tuple, it);
			memcpy(elem_start, value, TUP_TYPE_SIZE(type));
		}
	}
	va_end(ap);

	return next_operation(op);
}

dlog_pi_op_t *
next_operation(dlog_pi_op_t *op)
{
	uint32_t tup_size = SIZEOF_TUP(op->tuple);
	return (dlog_pi_op_t *) ((unsigned long) (op + 1) + tup_size - sizeof(dlog_tuple_t));
}

char *
get_pred_sql(dlog_pi_op_t *op)
{
	char *sql = (char *) get_var_length_ptr(&op->tuple, 1);
	return sql;
}

int
get_pred_sql_len(dlog_pi_op_t *op)
{
	int len = (int) get_var_length_ptr_len(&op->tuple, 1);
	if (len == 0)
		return len;

	return len - 1;
}

