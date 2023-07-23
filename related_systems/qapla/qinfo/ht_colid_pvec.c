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
#include "common/sym_xform.h"
#include "pol_vector.h"

#define PVEC_INDEX_SIZE	217

#define KEY(cs)	sym_field((cs), db_cid)

extern int int_index(void *key, int keylen, int modulo);
int colid_pvec_compare(void *entry, void *key, int keylen);
int colid_pvec_entry_free(void **entry);

// key: col_sym_t
// hash idx: hash(cs.db_cid)
// val: PVEC

int colid_pvec_compare(void *entry, void *key, int keylen, int exact_match)
{
	int ret;
	col_sym_t *e_cs = (col_sym_t *) entry;
	col_sym_t *cs = (col_sym_t *) key;

	int e_cid = sym_field(e_cs, db_cid);
	int cid = sym_field(cs, db_cid);
	if (e_cid != cid && e_cid != SPECIAL_CID)
		return HT_FAILURE;

	list_t *e_xf = &e_cs->xform_list;
	list_t *xf = &cs->xform_list;
	
	/* 
	 * if no transforms mentioned explicitly, all allowed
	 */
	if (!exact_match && list_empty(e_xf)) 
		return HT_SUCCESS;
	
	/* 
	 * policy col contains transforms => col can be accessed only under transforms
	 * but query col does not, so definitely disallowed
	 */
	if (!exact_match && list_empty(xf))
		return HT_FAILURE;

	ret = cmp_transform(e_xf, xf);
	return ret;
}

int
colid_pvec_free(void **hash_entry)
{
	if (!hash_entry)
		return HT_FAILURE;

	hash_entry_t *he = (hash_entry_t *) *hash_entry;
	if (!he)
		return HT_FAILURE;

	PVEC *pv = (PVEC *) he->val;
	col_sym_t *cs = (col_sym_t *) he->key;
	if (pv)
		free(pv);
	if (cs) {
		free_col_sym(&cs);
	}
	memset(he, 0, sizeof(hash_entry_t));
	list_init(&he->next);
	free(he);
	*hash_entry = NULL;

	return HT_SUCCESS;
}

hash_table_t *
ht_colid_init_pvec(int pvec_size)
{
	hash_table_t *pol_vec_index = 
		alloc_init_hash_table(pvec_size, &colid_ht_func);
	return pol_vec_index;
}

hash_table_t *
ht_colid_gen_schema_pvec(db_t *schema)
{
	void *key, *val;
	int keylen, lookup_key_len, val_len;
	int idx, lookup_key;
	PVEC bitvec = 0;
	col_sym_t *cs;
	int i, j, num_tab, num_col, total_col, cid, tid;

	num_tab = get_schema_num_tables(schema);
	total_col = get_schema_num_columns(schema);
	hash_table_t *pol_vec_index = 
		alloc_init_hash_table(total_col, &colid_ht_func);

	if (!pol_vec_index)
		return NULL;

	for (i = 0; i < num_tab; i++) {
		num_col = get_schema_num_col_in_table_id(schema, i);
		for (j = 0; j < num_col; j++) {
			cid = get_schema_global_col_id_in_table_id(schema, i, j);
			tid = get_schema_global_table_id(schema, i);
			
			cs = (col_sym_t *) malloc(sizeof(col_sym_t));
			init_col_sym(cs);
			set_col_sym_tid_cid(cs, tid, cid);
			key = (void *) cs;
			keylen = sizeof(col_sym_t);

			lookup_key = KEY(cs);
			lookup_key_len = sizeof(int);
			idx = hash(pol_vec_index, (void *)&lookup_key, lookup_key_len);

			val = malloc(sizeof(PVEC));
			*(PVEC *)val = bitvec;
			val_len = sizeof(PVEC);
			
			hash_entry_t *e = init_hash_entry(key, keylen, val, val_len, 0);
			add_hash_entry(pol_vec_index, idx, e);
		}
	}

	return pol_vec_index;
}

void 
ht_colid_free_schema_pvec(hash_table_t **pol_vec_index)
{
	if (!pol_vec_index || !*pol_vec_index)
		return;

	cleanup_hash_table(*pol_vec_index);
	free(*pol_vec_index);
	*pol_vec_index = NULL;
}

void 
ht_colid_print_schema_pvec(hash_table_t *pol_vec_index, FILE *f)
{
	if (!pol_vec_index) {
		fprintf(f, "pol vec index: NULL\n");
		return;
	}

	fprintf(f, "pol vec index -- size: %d\n", pol_vec_index->size);
	int i, cid;
	list_t *idx_head, *idx_it;
	hash_entry_t *e;
	col_sym_t *cs;
	for (i = 0; i < pol_vec_index->size; i++) {
		idx_head = &pol_vec_index->table[i];
		fprintf(f, "idx: %d\n", i);
		list_for_each_entry(e, idx_head, next) {
			cs = (col_sym_t *) e->key;
			cid = sym_field(cs, db_cid);
			if (cid == SPECIAL_CID && !list_empty(&cs->xform_list)) {
				list_t *xf_it = cs->xform_list.next;
				transform_t *tf = list_entry(xf_it, transform_t, xform_listp);
				if (tf->fn == COUNT)
					fprintf(f, "\t<COUNT,0x%lx>\n", *(PVEC *)e->val);
			} else {
				fprintf(f, "\t<%d,0x%lx>\n", sym_field(cs, db_cid), 
						*(PVEC *)e->val);
			}
		}
	}
}

void
ht_colid_add_pol_to_col(hash_table_t *pol_vec_index, int pid, col_sym_t *cs)
{
	hash_entry_t *he;
	void *key, *val;
	int keylen, lookup_key_len, val_len;
	int idx, lookup_key;
	PVEC bitvec = 0;
	PVEC *exists_value;

	lookup_key = KEY(cs);
	lookup_key_len = sizeof(int);
	idx = hash(pol_vec_index, (void *) &lookup_key, lookup_key_len);
	
	key = (void *) cs;
	keylen = sizeof(col_sym_t);
	bitvec = (PVEC) ((PVEC) 1 << pid);

	exists_value = (PVEC *) get_hash_value(pol_vec_index, idx, key, keylen, 1);
	if (!exists_value) {
		key = (void *) dup_col_sym(cs);
		keylen = sizeof(col_sym_t);
	
		val = malloc(sizeof(PVEC));
		*(PVEC *) val = bitvec;
		val_len = sizeof(PVEC);

		he = init_hash_entry(key, keylen, val, val_len, 0);
		add_hash_entry(pol_vec_index, idx, he);
	} else {
		*exists_value = *exists_value | bitvec;
	}
}

void 
ht_colid_add_pol_to_col_list(hash_table_t *pol_vec_index, int pid, list_t *cs_list)
{
	col_sym_t *cs;
	hash_entry_t *he;
	void *key, *val;
	int keylen, lookup_key_len, val_len;
	int idx, lookup_key;
	PVEC bitvec = 0;
	PVEC *exists_value;

	list_for_each_entry(cs, cs_list, col_sym_listp) {
		lookup_key = KEY(cs);
		lookup_key_len = sizeof(int);
		idx = hash(pol_vec_index, (void *) &lookup_key, lookup_key_len);
		
		key = (void *) cs;
		keylen = sizeof(col_sym_t);
		bitvec = (PVEC) ((PVEC) 1 << pid);

		exists_value = (PVEC *) get_hash_value(pol_vec_index, idx, key, keylen, 1);
		if (!exists_value) {
			key = (void *) dup_col_sym(cs);
			keylen = sizeof(col_sym_t);
		
			val = malloc(sizeof(PVEC));
			*(PVEC *) val = bitvec;
			val_len = sizeof(PVEC);

			he = init_hash_entry(key, keylen, val, val_len, 0);
			add_hash_entry(pol_vec_index, idx, he);
		} else {
			*exists_value = *exists_value | bitvec;
		}
	}
}

PVEC 
ht_colid_compute_query_pvec(hash_table_t *pol_vec_index, list_t *cs_list)
{
	PVEC polvec = -1; // start with 0xffffffff ffffffff
	PVEC *tmp;
	int keylen, lookup_key_len;
	int idx, lookup_key;
	void *key;

	col_sym_t *cs;
	list_for_each_entry(cs, cs_list, col_sym_listp) {
		lookup_key = KEY(cs);
		if (lookup_key < 0) {
			// for special temporary tables?
			continue;
		}
		lookup_key_len = sizeof(int);
		idx = hash(pol_vec_index, (void *) &lookup_key, lookup_key_len);

		key = (void *) cs;
		keylen = sizeof(col_sym_t);

		tmp = (PVEC *) get_hash_value(pol_vec_index, idx, key, keylen, 0);
		if (!tmp)
			return (PVEC) -1;

		polvec &= *tmp;
	}

	return polvec;
}

PVEC
ht_colid_get_col_pvec(hash_table_t *pol_vec_index, col_sym_t *cs)
{
	PVEC *tmp;
	int keylen, lookup_key_len;
	int idx, lookup_key;
	void *key;

	transform_t *tf;
	list_t *xf_it;
	int check_count = 0;

	lookup_key_len = sizeof(int);
	keylen = sizeof(col_sym_t);
	lookup_key = KEY(cs);
	if (lookup_key < 0) {
		// for special temporary tables?
		return -1;
	}

	// we do not look through series of transforms on the col sym,
	// but only at the first (outermost) transform
	if (!list_empty(&cs->xform_list)) {
		xf_it = cs->xform_list.next;
		tf = list_entry(xf_it, transform_t, xform_listp);
		if (tf->fn == COUNT) {
			lookup_key = SPECIAL_CID;
			check_count = 1;
		}
	}
	key = (void *) cs;

	idx = hash(pol_vec_index, (void *) &lookup_key, lookup_key_len);
	tmp = (PVEC *) get_hash_value(pol_vec_index, idx, key, keylen, 0);
	if (!tmp && !check_count)
		return (PVEC) -1;

	if (!tmp && check_count) {
		lookup_key = KEY(cs);
		key = (void *) cs;
		idx = hash(pol_vec_index, (void *) &lookup_key, lookup_key_len);
		tmp = (PVEC *) get_hash_value(pol_vec_index, idx, key, keylen, 0);
		if (!tmp)
			return (PVEC) -1;
	}

	return *tmp;
}

pvec_fn_t ht_colid_pvec = {
	ht_colid_init_pvec,
	ht_colid_gen_schema_pvec,
	ht_colid_free_schema_pvec,
	ht_colid_print_schema_pvec,
	ht_colid_add_pol_to_col_list,
	ht_colid_add_pol_to_col,
	ht_colid_compute_query_pvec,
	ht_colid_get_col_pvec
};

hash_fn_t colid_ht_func = {
	int_index,
	colid_pvec_compare,
	colid_pvec_free
};

