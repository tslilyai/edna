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
#include "utils/hashtable.h"
#include "common/db.h"
#include "common/col_sym.h"
#include "pol_vector.h"

#define PVEC_INDEX_SIZE	217

extern int murmur_hash(void *key, int key_len, int modulo);
int colname_pvec_compare(void *entry, void *key, int keylen);
int colname_pvec_free(void **entry);

int colname_pvec_compare(void *entry, void *key, int keylen)
{
	// not implemented
	return HT_SUCCESS;
}

int colname_pvec_free(void **entry)
{
	// not implemented
	return HT_SUCCESS;
}

hash_table_t *
ht_colname_init_pvec(int pvec_size)
{
	hash_table_t *pol_vec_index = 
		alloc_init_hash_table(pvec_size, &colname_ht_func);
	return pol_vec_index;
}

hash_table_t *
ht_colname_gen_schema_pvec(db_t *schema)
{
	char buf[512];
	char *key;
	int key_len = 0;
	int idx = 0;
	PVEC bitvec = 0;
	int i, j, num_tab, num_col;
	char *tname, *cname;

	num_tab = get_schema_num_tables(schema);
	hash_table_t *pol_vec_index = 
		alloc_init_hash_table(PVEC_INDEX_SIZE, &colname_ht_func);
	for (i = 0; i < num_tab; i++) {
		tname = get_schema_table_name(schema, i);
		num_col = get_schema_num_col_in_table_id(schema, i);
		for (j = 0; j < num_col; j++) {
			cname = get_schema_col_name_in_table_id(schema, i, j);
			memset(buf, 0, 512);
			sprintf(buf, "%s.%s", tname, cname);
			key = buf;
			key_len = strnlen(key, 512);
			idx = hash(pol_vec_index, key, key_len);

			hash_entry_t *e = init_hash_entry(key, key_len, &bitvec, sizeof(PVEC), 1);
			add_hash_entry(pol_vec_index, idx, e);
		}
	}

	return pol_vec_index;
}

void 
ht_colname_free_schema_pvec(hash_table_t **pol_vec_index)
{
	if (!pol_vec_index || !*pol_vec_index)
		return;

	cleanup_hash_table(*pol_vec_index);
	free(*pol_vec_index);
	*pol_vec_index = NULL;
}

void 
ht_colname_print_schema_pvec(hash_table_t *pol_vec_index, FILE *f)
{
	if (!pol_vec_index) {
		fprintf(f, "pol vec index: NULL\n");
		return;
	}

	fprintf(f, "pol vec index -- size: %d\n", pol_vec_index->size);
	int i;
	list_t *idx_head, *idx_it;
	hash_entry_t *e;
	for (i = 0; i < pol_vec_index->size; i++) {
		idx_head = &pol_vec_index->table[i];
		fprintf(f, "idx: %d\n", i);
		list_for_each_entry(e, idx_head, next) {
			fprintf(f, "\t<%s,0x%x>\n", (char *)e->key, *(PVEC *)e->val);
		}
	}
}

void
ht_colname_add_pol_to_col(hash_table_t *pol_vec_index, int pid, col_sym_t *cs)
{
	hash_entry_t *he;
	char buf[512];
	char *key;
	int keylen = 0;
	PVEC value;
	PVEC *exists_value;
	int idx = 0;

	memset(buf, 0, 512);
	sprintf(buf, "%s.%s", sym_field(cs, db_tname), sym_field(cs, db_cname));
	key = buf;
	keylen = strnlen(key, 512);
	value = (1 << pid);
	idx = hash(pol_vec_index, key, keylen);
	exists_value = (PVEC *) get_hash_value(pol_vec_index, idx, key, keylen);
	if (!exists_value) {
		he = init_hash_entry(key, keylen, (void *) &value, sizeof(PVEC), 1);
		add_hash_entry(pol_vec_index, idx, he);
	} else {
		*exists_value = *exists_value | value;
	}
}

void 
ht_colname_add_pol_to_col_list(hash_table_t *pol_vec_index, int pid, list_t *cs_list)
{
	col_sym_t *cs;
	hash_entry_t *he;
	char buf[512];
	char *key;
	int keylen = 0;
	PVEC value;
	PVEC *exists_value;
	int idx = 0;
	list_for_each_entry(cs, cs_list, col_sym_listp) {
		memset(buf, 0, 512);
		sprintf(buf, "%s.%s", sym_field(cs, db_tname), sym_field(cs, db_cname));
		key = buf;
		keylen = strnlen(key, 512);
		value = (1 << pid);
		idx = hash(pol_vec_index, key, keylen);
		exists_value = (PVEC *) get_hash_value(pol_vec_index, idx, key, keylen);
		if (!exists_value) {
			he = init_hash_entry(key, keylen, (void *) &value, sizeof(PVEC), 1);
			add_hash_entry(pol_vec_index, idx, he);
		} else {
			*exists_value = *exists_value | value;
		}
	}
}

PVEC
ht_colname_get_col_pvec(hash_table_t *pol_vec_index, col_sym_t *cs)
{
	PVEC *tmp;
	int keylen;
	int idx;
	void *key;
	char buf[512];
	memset(buf, 0, 512);
	sprintf(buf, "%s.%s", sym_field(cs, db_tname), sym_field(cs, db_cname));
	key = buf;
	keylen = strnlen(buf, 512);

	idx = hash(pol_vec_index, key, keylen);
	tmp = (PVEC *) get_hash_value(pol_vec_index, idx, key, keylen);
	if (!tmp)
		return (PVEC) -1;

	return *tmp;
}

PVEC 
ht_colname_compute_query_pvec(hash_table_t *pol_vec_index, list_t *cs_list)
{
	PVEC polvec = -1; // start with 0xffffffff ffffffff
	PVEC tmp = 0;
	int i, j, keylen, idx;
	char buf[512];
	char *key;
	col_sym_t *cs;
	list_for_each_entry(cs, cs_list, col_sym_listp) {
		memset(buf, 0, 512);
		sprintf(buf, "%s.%s", sym_field(cs, db_tname), sym_field(cs, db_cname));
		key = buf;
		keylen = strnlen(key, 512);
		idx = hash(pol_vec_index, key, keylen);
		tmp = *(PVEC *) get_hash_value(pol_vec_index, idx, key, keylen);
		polvec &= tmp;
	}

	return polvec;
}

pvec_fn_t ht_colname_pvec = {
	ht_colname_init_pvec,
	ht_colname_gen_schema_pvec,
	ht_colname_free_schema_pvec,
	ht_colname_print_schema_pvec,
	ht_colname_add_pol_to_col_list,
	ht_colname_add_pol_to_col,
	ht_colname_compute_query_pvec,
	ht_colname_get_col_pvec
};

hash_fn_t colname_ht_func = {
	murmur_hash,
	colname_pvec_compare,
	colname_pvec_free
};

static PVEC __polvec = 0;

PVEC get_query_pol_history()
{
	return __polvec;
}

void update_query_pol_history(PVEC polvec)
{
	__polvec |= polvec;
}
