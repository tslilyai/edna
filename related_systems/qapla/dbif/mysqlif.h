/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __MYSQLIF_H__
#define __MYSQLIF_H__

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct mysql_result {
		void *priv;
	} mysql_result_t;

	int init_mysql_conn(void **conn, char *db_name);
	void close_mysql_conn(void *conn);
	int query_mysql_conn(void *conn, char *q, int qlen);
	int query_mysql_result(void *conn, void **res_buf, int *nrow, int *ncol);
	int query_mysql_result2(void *conn, mysql_result_t *mres, int *nrow, int *ncol);
	void free_mysql_result(void **res_buf);
	void free_mysql_result2(mysql_result_t *mres);
	int result_next_row_col(void *res_buf, int cidx, char **buf, int *len, int *type);
	int result_next_row_col_arr(void *res_buf, int *col_arr, int ncol, char **buf,
			int *len, int *type);

#ifdef __cplusplus
}
#endif

#endif /* __MYSQLIF_H__ */
