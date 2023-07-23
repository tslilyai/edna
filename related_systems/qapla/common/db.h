/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QAPLA_SCHEMA_H__
#define __QAPLA_SCHEMA_H__

#define MAX_TAB 100
#define MAX_COL 500

// max length of database identifiers
#ifndef MAX_NAME_LEN
#define MAX_NAME_LEN	128
#endif

#ifdef __cplusplus
extern "C" {
#endif

enum {
	COUNT = 0,
	SUM,
	AVG,
	STD,
	MAX,
	MIN,
	AFFIL,
	COUNTRY,
	TOPIC,
	MAX_AGGR_FUNC
};

/* 
 * defines the COUNT state in policy metadata
 * set to a value that does not collide with the cid in schema
 */
#define SPECIAL_CID	1000
#define DUAL_TID	2000
#define DUAL_TNAME "dual"

#define PROJECT 	0x000001
#define TABLE 		0x000002
#define JOIN_COND 0x000004
#define FILTER		0x000008
#define GROUP			0x000010
#define HAVING		0x000020
#define ORDER			0x000040
#define MAX_EXPR_TYPE 6

typedef struct db {
	void *priv;
} db_t;

void init_db(db_t *db, const char *dbname, int maxtab, int maxcol,
		int (*load_schema_fn) (db_t *db));
void cleanup_db(db_t *db);
void print_db(db_t *db, FILE *f);

int add_table(db_t *db, const char *tname, int *tid);
int add_col_in_table_id(db_t *db, int tid, const char *cname, int *cid);
int add_col_in_table(db_t *db, const char *tname, const char *cname, int *tid, int *cid);

int get_schema_num_tables(db_t *schema);
int get_schema_num_columns(db_t *schema);
int get_schema_table_id(db_t *schema, char *tname);
int get_schema_global_table_id(db_t *schema, int tid);
char *get_schema_table_name(db_t *schema, int tid);

int get_schema_num_col_in_table_id(db_t *schema, int tid);
int get_schema_col_id_in_table_id(db_t *schema, int tid, char *cname);
int get_schema_col_id_in_table(db_t *schema, char *tname, char *cname);
int get_schema_global_col_id_in_table_id(db_t *schema, int tid, int cid);
char *get_schema_col_name_in_table_id(db_t *schema, int tid, int cid);

#ifdef __cplusplus
}
#endif
#endif /* __QAPLA_SCHEMA_H__ */
