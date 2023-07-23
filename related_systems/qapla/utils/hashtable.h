/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __HASHTABLE_H__
#define __HASHTABLE_H__

#include "list.h"

#ifdef __cplusplus
extern "C" {
#endif

#define HT_SUCCESS 0
#define HT_FAILURE 1

	typedef struct hash_fn {
		int (*hash_func) (void *key, int key_len, int modulo);
		int (*hash_val_compare) (void *entry, void *key, int keylen, int exact);
		int (*hash_entry_free) (void **entry);
	} hash_fn_t;

	extern hash_fn_t colid_ht_func;
	extern hash_fn_t colname_ht_func;
	extern hash_fn_t log_table_func;
	
	typedef struct hash_entry {
		void *key;
		void *val;
		int key_len;
		int val_len;
		list_t next;
	} hash_entry_t;

	typedef struct hash_table {
		int size;
		list_t *table;
		hash_fn_t *ht_func;
	} hash_table_t;
	
	hash_table_t *alloc_init_hash_table(int size, hash_fn_t *ht_func_p);
	int init_hash_table(hash_table_t *ht, int size, hash_fn_t *ht_func_p);
	void cleanup_hash_table(hash_table_t *ht);

	void add_hash_key_value(hash_table_t *ht, void *key, void *value);
	void add_hash_entry(hash_table_t *ht, int idx, hash_entry_t *e);
	void *get_hash_value(hash_table_t *ht, int idx, void *key, int keylen, int exact);
	hash_entry_t *init_hash_entry(void *key, int key_len, void *value, int val_len, int alloc);
	void free_hash_entry(hash_entry_t **he);
	int hash(hash_table_t *ht, void *key, int keylen);

#ifdef __cplusplus
}
#endif

#endif /* __HASHTABLE_H__ */
