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
#include <assert.h>

#include "db.h"

typedef struct col {
	char *name;
	int id;
} col_t;

typedef struct tab {
	char *name;
	int id;
	int num_col;
	col_t *a_col;
} tab_t;

typedef struct db_int {
	char *name;
	int max_tab;
	int max_col;
	int num_tab;
	int total_col;
	tab_t *a_tab;
} db_int_t;

void 
get_aggr_name(char *buf, int buflen, int aggr)
{
	if (!buf)
		return;

	memset(buf, 0, buflen);

	switch (aggr) {
		case COUNT:
			sprintf(buf, "count");
			break;
		case SUM:
			sprintf(buf, "sum");
			break;
		case AVG:
			sprintf(buf, "avg");
			break;
		case STD:
			sprintf(buf, "stddev");
			break;
		case MAX:
			sprintf(buf, "max");
			break;
		case MIN:
			sprintf(buf, "min");
			break;
		case AFFIL:
			sprintf(buf, "affil");
			break;
		case TOPIC:
			sprintf(buf, "topic");
			break;
	}
}

void 
print_db(db_t *db, FILE *f)
{
	if (!db)
		return;

	db_int_t *db_p = (db_int_t *) db->priv;
	char ttab[32];
	char ctab[32];
	char aggr[32];
	memset(ttab, 0, 32);
	memset(ctab, 0, 32);
	memset(aggr, 0, 32);

	sprintf(ttab, " ");
	sprintf(ctab, "\t");

	if (!f)
		return;

	fprintf(f, "%s\n", db_p->name);
	int i, j;
	char prefix[32];
	memset(prefix, 0, 32);
	char buf[MAX_NAME_LEN];
	memset(buf, 0, MAX_NAME_LEN);
	for (i = 0; i < db_p->num_tab; i++) {
		tab_t *t = &db_p->a_tab[i];
		fprintf(f, " %d: %s\n", t->id, t->name);
		for (j = 0; j < t->num_col; j++) {
			col_t *c = &t->a_col[j];
			sprintf(buf, "\t%d: %s\n", c->id, c->name);
			//sprintf(prefix, "%d", c->id);
			//get_xform_list(buf, prefix, &c->xf_list, c->name);
			fprintf(f, "\t%s\n", buf);
		}
	}

	fprintf(f, "\n");
}

void 
init_db (db_t *db, const char *db_name, int maxtab, int maxcol,
		int (*load_schema_fn) (db_t *db))
{
	db_int_t *db_p = (db_int_t *) malloc(sizeof(db_int_t));
	memset(db_p, 0, sizeof(db_int_t));
	db_p->name = strdup(db_name);
	db_p->num_tab = 0;
	db_p->total_col = 0;
	db_p->max_tab = maxtab;
	db_p->max_col = maxcol;
	db_p->a_tab = (tab_t *) malloc(sizeof(tab_t) * maxtab);
	memset(db_p->a_tab, 0, sizeof(tab_t) * maxtab);
	int i, j;
	for (i = 0; i < maxtab; i++) {
		tab_t *t = &db_p->a_tab[i];
		t->a_col = (col_t *) malloc(sizeof(col_t) * maxcol);
		memset(t->a_col, 0, sizeof(col_t) * maxcol);
		for (j = 0; j < maxcol; j++) {
			col_t *c = &t->a_col[j];
		}
	}

	db->priv = (void *) db_p;

	if (load_schema_fn)
		load_schema_fn(db);
}

void 
cleanup_db(db_t *db)
{
	int i, j;
	tab_t *t;
	col_t *c;
	if (!db || !db->priv)
		return;

	db_int_t *db_p = (db_int_t *) db->priv;
	for (i = 0 ; i < db_p->max_tab; i++) {
		t = &db_p->a_tab[i];
		for (j = 0 ; j < t->num_col; j++) {
			c = &t->a_col[j];
			if (c->name) {
				free(c->name);
				c->name = NULL;
			}
			memset(c, 0, sizeof(col_t));
		}

		if (t->name) {
			free(t->name);
			t->name = NULL;
		}
		if (t->a_col) {
			free(t->a_col);
			t->a_col = NULL;
		}
		memset(t, 0, sizeof(tab_t));
	}

	if (db_p->name) {
		free(db_p->name);
		db_p->name = NULL;
	}

	if (db_p->a_tab) {
		free(db_p->a_tab);
		db_p->a_tab = NULL;
	}

	memset(db_p, 0, sizeof(db_t));
	free(db_p);
	db->priv = NULL;
}

int 
add_table(db_t *db, const char *tname, int *tid)
{
	if (!db)
		return -1;

	db_int_t *db_p = (db_int_t *) db->priv;

	int i;
	tab_t *t = NULL;
	for (i = 0; i < db_p->num_tab; i++)
	{
		t = &db_p->a_tab[i];
		if (!t->name)
			break;

		if (strlen(t->name) != strlen(tname))
			continue;

		if (strncmp(t->name, tname, strlen(t->name)) == 0)
		{
			// table already exists in the schema
			*tid = i;
			return 0;
		}
	}

	if (i >= MAX_TAB)
		return -1;

	t = &db_p->a_tab[i];
	if (!t->name) {
		t->name = strdup(tname);
		t->id = *tid = db_p->num_tab++;
	}
	return 0;
}

int 
add_col_in_table_id(db_t *db, int tid, const char *cname, int *cid)
{
	if (!db)
		return -1;

	db_int_t *db_p = (db_int_t *) db->priv;
	if (tid > db_p->num_tab)
		return -1;

	tab_t *t = &db_p->a_tab[tid];
	col_t *c;
	int i;
	for (i = 0; i < t->num_col; i++) {
		c = &t->a_col[i];
		if (!c->name)
			break;

		if (strlen(c->name) != strlen(cname))
			continue;

		if (strncmp(c->name, cname, strlen(c->name)) == 0) {
			// column already exists in table
			*cid = i;
			return 0;
		}
	}

	if (i >= MAX_COL)
		return -1;

	c = &t->a_col[i];
	t->num_col++;
	c->name = strdup(cname);
	c->id = *cid = db_p->total_col++;

	return 0;
}

int 
add_col_in_table(db_t *db, const char *tname, const char *cname, int *tid, int *cid)
{
	int ret = add_table(db, tname, tid);
	if (ret < 0)
		return ret;

	return add_col_in_table_id(db, *tid, cname, cid);
}

// == get functions ==
int
get_schema_num_tables(db_t *schema)
{
	db_int_t *schema_p = (db_int_t *) schema->priv;
	return schema_p->num_tab;
}

int
get_schema_num_columns(db_t *schema)
{
	db_int_t *schema_p = (db_int_t *) schema->priv;
	return schema_p->total_col;
}

int 
get_schema_table_id(db_t *schema, char *tname)
{
	int i = 0;
	db_int_t *schema_p = (db_int_t *) schema->priv;
	int num_tab = schema_p->num_tab;
	tab_t *schema_t = NULL;
	for (i = 0; i < num_tab; i++) {
		schema_t = &schema_p->a_tab[i];
		if (!schema_t->name || (strlen(schema_t->name) != strlen(tname)))
			continue;

		if (strncmp(schema_t->name, tname, strlen(schema_t->name)) == 0)
			return schema_t->id;
	}

	return -1;
}

int
get_schema_global_table_id(db_t *schema, int tid)
{
	db_int_t *schema_p = (db_int_t *) schema->priv;
	if (tid >= schema_p->num_tab)
		return -1;

	tab_t *schema_t = &schema_p->a_tab[tid];
	return schema_t->id;
}

char *
get_schema_table_name(db_t *db, int tid)
{
	if (!db)
		return NULL;

	db_int_t *db_p = (db_int_t *) db->priv;
	if (tid >= db_p->num_tab)
		return NULL;

	return db_p->a_tab[tid].name;
}

int
get_schema_num_col_in_table_id(db_t *db, int tid)
{
	if (!db)
		return -1;

	db_int_t *db_p = (db_int_t *) db->priv;
	if (tid >= db_p->num_tab)
		return -1;

	return db_p->a_tab[tid].num_col;
}

int 
get_schema_col_id_in_table_id(db_t *schema, int tid, char *cname)
{
	if (tid < 0 || !cname || !schema)
		return -1;

	int i = 0, num_col = 0;
	db_int_t *schema_p = (db_int_t *) schema->priv;
	tab_t *schema_t = &schema_p->a_tab[tid];
	col_t *schema_c = NULL;
	num_col = schema_t->num_col;
	for (i = 0; i < num_col; i++) {
		schema_c = &schema_t->a_col[i];
		if (!schema_c->name || (strlen(schema_c->name) != strlen(cname)))
			continue;

		if (strncmp(schema_c->name, cname, strlen(schema_c->name)) == 0)
			return schema_c->id;
	}

	return -1;
}

int 
get_schema_col_id_in_table(db_t *schema, char *tname, char *cname)
{
	int i = 0, j = 0, num_col = 0;
	db_int_t *schema_p = (db_int_t *) schema->priv;
	int num_tab = schema_p->num_tab;
	tab_t *schema_t = NULL;
	col_t *schema_c = NULL;
	for (i = 0; i < num_tab; i++) {
		schema_t = &schema_p->a_tab[i];
		if (!schema_t->name || (strlen(schema_t->name) != strlen(tname)))
			continue;

		if (strncmp(schema_t->name, tname, strlen(schema_t->name)) == 0)
			return get_schema_col_id_in_table_id(schema, i, cname);
	}

	return -1;
}

int
get_schema_global_col_id_in_table_id(db_t *db, int tid, int cid)
{
	if (!db)
		return -1;

	db_int_t *db_p = (db_int_t *) db->priv;
	if (tid >= db_p->num_tab)
		return -1;

	tab_t *t = &db_p->a_tab[tid];
	if (cid >= t->num_col)
		return -1;

	return t->a_col[cid].id;
}

char *
get_schema_col_name_in_table_id(db_t *db, int tid, int cid)
{
	if (!db)
		return NULL;

	db_int_t *db_p = (db_int_t *) db->priv;
	if (tid >= db_p->num_tab)
		return NULL;

	tab_t *t = &db_p->a_tab[tid];
	if (cid >= t->num_col)
		return NULL;

	return t->a_col[cid].name;
}

