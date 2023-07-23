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
#include <mysql.h>

#include "common/config.h"
#include "common/qapla_policy.h"
#include "conn.h"
#include "mysqlif.h"

typedef struct mysql_result_int {
	int num_col;
	int num_row;
	int *types;
	int *sizes;
	char ***data;
	void *res_buf;
} mysql_result_int_t;

int
init_mysql_conn(void **conn, char *db_name)
{
	if (!conn)
		return CONN_NOT_CONNECTED;

	MYSQL *sock;
	sock = mysql_init(0);
	if (!sock) {
		fprintf(stderr, "Couldn't initialize mysql struct\n");
		return CONN_NOT_CONNECTED;
	}

	mysql_options(sock, MYSQL_READ_DEFAULT_GROUP, "connect");
	if (!mysql_real_connect(sock, NULL, DB_ADMIN, "passw0rd", NULL, 0, NULL, 0)) {
		fprintf(stderr, "Couldn't connect to engine!\n%s\n", mysql_error(sock));
		perror("");
		return CONN_NOT_CONNECTED;
	}

	//printf("char set: %s\n", mysql_character_set_name(sock));
	mysql_set_character_set(sock, "utf8");

	//sock->reconnect = 1;
	if (mysql_select_db(sock, db_name)) {
		fprintf(stderr, "Couldn't select database %s, error: %s\n", 
				db_name, mysql_error(sock));
		mysql_close(sock);
		return CONN_NOT_CONNECTED;
	}

	if (mysql_change_user(sock, DB_ADMIN, "passw0rd", db_name)) {
		fprintf(stderr, "Couldn't change user to qapla, db: %s, error: %s\n",
				db_name, mysql_error(sock));
		mysql_close(sock);
		return CONN_NOT_CONNECTED;
	}

	*conn = (void *) sock;
	return CONN_SUCCESS;
}

void
close_mysql_conn(void *conn)
{
	if (!conn)
		return;

	MYSQL *sock = (MYSQL *) conn;
	mysql_close(sock);
	mysql_server_end();
}

int
query_mysql_conn(void *conn, char *q, int qlen)
{
	if (!conn)
		return CONN_NOT_CONNECTED;

	MYSQL *sock = (MYSQL *) conn;
	//char *escaped_q = (char *) malloc(2*qlen + 1);
	//memset(escaped_q, 0, 2*qlen+1);
	//uint64_t escaped_qlen = mysql_real_escape_string(sock, escaped_q, q, qlen);
	//if (escaped_qlen == UINT64_MAX)
	//	return CONN_QUERY_ERROR;

	//if (mysql_real_query(sock, escaped_q, escaped_qlen)) {
	if (mysql_real_query(sock, q, qlen)) {
		fprintf(stderr, "Query failure (%s)\n", mysql_error(sock));
		return CONN_QUERY_ERROR;
	}

	//fprintf(stderr, "Query success\n");
	return CONN_SUCCESS;
}

int
query_mysql_result(void *conn, void **buf, int *nrow, int *ncol)
{
	MYSQL_RES *res;
	MYSQL *sock = (MYSQL *) conn;
	res = mysql_store_result(sock);
	if (!res)
		return CONN_QUERY_ERROR;

	int num_row = mysql_num_rows(res);
	int num_col = mysql_num_fields(res);
#if DEBUG
	printf("#cols: %d, #rows: %d\n", num_col, num_row);
#endif
	*buf = (void *) res;
	*nrow = num_row;
	*ncol = num_col;
	return CONN_SUCCESS;
}

int
query_mysql_result2(void *conn, mysql_result_t *mres, int *nrow, int *ncol)
{
	MYSQL_RES *res;
	MYSQL *sock = (MYSQL *) conn;
	res = mysql_store_result(sock);
	if (!res)
		return CONN_QUERY_ERROR;

	mysql_result_int_t *mres_int = (mysql_result_int_t *) malloc(sizeof(mysql_result_int_t));
	memset(mres_int, 0, sizeof(mysql_result_int_t));
	mres->priv = (void *) mres_int;

	int num_row = mysql_num_rows(res);
	int num_col = mysql_num_fields(res);
#if DEBUG
	printf("#cols: %d, #rows: %d\n", num_col, num_row);
#endif

	mres_int->num_col = num_col;
	mres_int->num_row = num_row;
	mres_int->types = (int *) malloc(sizeof(int) * num_col);
	mres_int->sizes = (int *) malloc(sizeof(int) * num_col);
	mres_int->data = (char ***) malloc(sizeof(char **) * num_row);
	mres_int->res_buf = (void *) res;

	int col_it, row_it = 0;
	MYSQL_FIELD *field;
	MYSQL_ROW row;

	do {
		row = mysql_fetch_row(res);
		if (!row)
			break;

		mres_int->data[row_it] = row;
		uint64_t *lengths = mysql_fetch_lengths(res);
		if (!lengths)
			return CONN_END_OF_RESULT;

		for (col_it = 0; col_it < num_col; col_it++) {
			mres_int->sizes[col_it] = lengths[col_it];
			field = mysql_fetch_field_direct(res, col_it);
			mres_int->types[col_it] = field->type;
		}
	} while (row != NULL);
	

	*nrow = num_row;
	*ncol = num_col;
	return CONN_SUCCESS;
}

int
result_next_row_col(void *res_buf, int cidx, char **buf, int *len, int *type)
{
	MYSQL_RES *res;
	MYSQL_ROW row;
	MYSQL_FIELD *field;
	uint64_t *lengths;

	res = (MYSQL_RES *) res_buf;
	row = mysql_fetch_row(res);
	if (!row)
		return CONN_END_OF_RESULT;

	lengths = mysql_fetch_lengths(res);
	if (!lengths)
		return CONN_END_OF_RESULT;

	field = mysql_fetch_field_direct(res, cidx);

	*buf = row[cidx];
	*len = lengths[cidx];
	*type = field->type;

	return CONN_SUCCESS;
}

int
result_next_row_col_arr(void *res_buf, int *col_arr, int ncol, char **buf,
		int *len, int *type)
{
	int i,cidx;
	MYSQL_RES *res;
	MYSQL_ROW row;
	MYSQL_FIELD *field;
	uint64_t *lengths;

	res = (MYSQL_RES *) res_buf;
	row = mysql_fetch_row(res);
	if (!row)
		return CONN_END_OF_RESULT;

	lengths = mysql_fetch_lengths(res);
	if (!lengths)
		return CONN_END_OF_RESULT;

	for (i = 0; i < ncol; i++) {
		cidx = col_arr[i];
		field = mysql_fetch_field_direct(res, cidx);
		buf[i] = row[cidx];
		len[i] = lengths[cidx];
		type[i] = field->type;
	}

	return CONN_SUCCESS;
}

void
free_mysql_result(void **res_buf)
{
	if (!res_buf || !(*res_buf))
		return;

	MYSQL_RES *res = (MYSQL_RES *) *res_buf;
	mysql_free_result(res);
	*res_buf = NULL;
}

void
free_mysql_result2(mysql_result_t *mres)
{
	int i;
	mysql_result_int_t *mres_int = (mysql_result_int_t *) mres->priv;
	MYSQL_RES *res = (MYSQL_RES *) mres_int->res_buf;
	mysql_free_result(res);
	if (mres_int->types)
		free(mres_int->types);
	if (mres_int->sizes)
		free(mres_int->sizes);

	if (mres_int->data) {
#if 0
		// only pointer to row returned from result
		for (i = 0; i < mres_int->num_row; i++) {
			if (mres_int->data[i])
				free(mres_int->data[i]);
		}
#endif
		free(mres_int->data);
	}

	memset(mres_int, 0, sizeof(mysql_result_int_t));
	free(mres_int);
	mres->priv = NULL;
}
