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
#include <stdarg.h>
#include <ctype.h>

#include "common/config.h"
#include "sql_pred.h"

const char *op_names[MAX_NUM_OP] = {
	">", ">=", "<", "<=", "=", "!="
};

char *get_op_name(int op_id)
{
	if (op_id < 0 || op_id > MAX_NUM_OP)
		return NULL;

	return (char *) op_names[op_id];
}

char * 
add_pred(char *op, char *pbuf, int pred_len)
{
	memset(op, 0, pred_len+1);
	sprintf(op, "%s", pbuf);
	return op + (pred_len+1);
}

void
combine_pred(char *qbuf, int *qlen, char **str_list, int n_str, int connector)
{
	char *ptr = qbuf;
	int i, len = 0;
	sprintf(ptr, "(");
	len = strlen(ptr);
	for (i = 0; i < n_str - 1; i++) {
		sprintf(ptr + len, "%s", str_list[i]);
		len = strlen(ptr);
		if (connector == 0)
			sprintf(ptr + len, " AND ");
		else
			sprintf(ptr + len, " OR ");
		len = strlen(ptr);
	}
	sprintf(ptr + len, "%s)", str_list[i]);
	*qlen = strlen(ptr);
}

void
create_view_query(char *qbuf, int *qlen, char *tabname, char **str_list, 
		int n_str, int connector)
{
	char *buf = (char *) malloc(1024);
	memset(buf, 0, 1024);
	char *ptr = buf;
	int i, len = 0;
	sprintf(ptr, "(select * from %s where ", tabname);
	len = strlen(ptr);

	for (i = 0; i < n_str - 1; i++) {
		sprintf(ptr + len, "%s", str_list[i]);
		len = strlen(ptr);
		int is_and = connector & (1 << i);
		if (is_and == 0)
			sprintf(ptr + len, " AND ");
		else
			sprintf(ptr + len, " OR ");
		len = strlen(ptr);
	}
	sprintf(ptr + len, "%s)", str_list[i]);
	len = strlen(ptr);

#if CONFIG_POL_STORE == CONFIG_PSTORE_DB
	printf("sql: %s, len: %d\n", buf, len);
	convert_str_to_hex(buf, len, qbuf);
	*qlen = 2*(len+1); // caller is adding +1
#else
	memcpy(qbuf, buf, len);
	*qlen = len;
#endif
	free(buf);
}

