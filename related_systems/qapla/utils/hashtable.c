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

#include "hashtable.h"

hash_table_t *
alloc_init_hash_table(int size, hash_fn_t *ht_func_p)
{
	hash_table_t *ht = (hash_table_t *) malloc(sizeof(hash_table_t));
	if (!ht)
		return NULL;

	int ret = 0;
	ret = init_hash_table(ht, size, ht_func_p);
	if (ret == -1) {
		free(ht);
		ht = NULL;
	}

	return ht;
}

int
init_hash_table(hash_table_t *ht, int size, hash_fn_t *ht_func_p)
{
	if (!ht)
		return -2;

	memset(ht, 0, sizeof(hash_table_t));

	ht->size = size;
	ht->ht_func = ht_func_p;
	ht->table = (list_t *) malloc(sizeof(list_t) * size);
	if (!ht->table) {
		return -1;
	}

	int i;
	for (i = 0; i < size; i++) {
		list_init(&(ht->table[i]));
	}

	return 0;
}

void 
cleanup_hash_table(hash_table_t *ht)
{
	if (!ht)
		return;

	list_t *idx_head, *idx_it, *next_idx_it;
	hash_entry_t *e;
	int i;
	for (i = 0; i < ht->size; i++) {
		idx_head = &ht->table[i];
		idx_it = idx_head->next;
		while (idx_it != idx_head) {
			e = list_entry(idx_it, hash_entry_t, next);
			next_idx_it = idx_it->next;
			list_remove(idx_it);
			ht->ht_func->hash_entry_free((void **) &e);
			idx_it = next_idx_it;
		}
	}

	if (ht->table)
		free(ht->table);

	memset(ht, 0, sizeof(hash_table_t));

	return;
}

hash_entry_t *
init_hash_entry(void *key, int key_len, void *value, int val_len, int alloc)
{
	hash_entry_t *he = (hash_entry_t *) malloc(sizeof(hash_entry_t));
	memset(he, 0, sizeof(hash_entry_t));

	if (alloc) {
		he->key = malloc(key_len);
		he->val = malloc(val_len);
		memcpy(he->key, key, key_len);
		memcpy(he->val, value, val_len);
	} else {
		he->key = key;
		he->val = value;
	}
	he->key_len = key_len;
	he->val_len = val_len;
	list_init(&he->next);

	return he;
}

void 
add_hash_entry(hash_table_t *ht, int idx, hash_entry_t *e)
{
	list_t *head = &ht->table[idx];
	list_insert(head, &e->next);
}

void *
get_hash_value(hash_table_t *ht, int idx, void *key, int keylen, int exact_match)
{
	//int idx = hash(ht, key, keylen);
	list_t *head = &ht->table[idx];
	hash_entry_t *he;
	hash_fn_t *hfn = ht->ht_func;
	list_for_each_entry(he, head, next) {
		if (hfn->hash_val_compare(he->key, key, keylen, exact_match) == HT_SUCCESS)
			return he->val;
	}

	return NULL;
}

int 
hash(hash_table_t *ht, void *key, int keylen)
{
	return ht->ht_func->hash_func(key, keylen, ht->size);
}
