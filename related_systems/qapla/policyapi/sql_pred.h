/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QAPLA_SQL_POL_H__
#define __QAPLA_SQL_POL_H__

#ifdef __cplusplus
extern "C" {
#endif

	enum {
		OP_GT = 0,
		OP_GE,
		OP_LT,
		OP_LE,
		OP_EQ,
		OP_NE,
		MAX_NUM_OP
	};

	extern const char *op_names[MAX_NUM_OP];
	char *get_op_name(int op_id);
	char *add_pred(char *op, char *pbuf, int pred_len);
	void combine_pred(char *qbuf, int *qlen, char **str_list, int n_str, 
			int connector);
	void create_view_query(char *qbuf, int *qlen, char *tabname, char **str_list,
			int n_str, int connector);

#ifdef __cplusplus
}
#endif

#endif /* __QAPLA_SQL_POL_H__ */
