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
#include <stdint.h>
#include <assert.h>

#include "utils/hashtable.h"
#include "common/query.h"
#include "query_cache.h"

extern int murmur_hash(void *key, int keylen, int modulo);
int qcache_elem_compare(void *entry, void *key, int keylen, int exact_match)
{
	qcache_elem_t *e = (qcache_elem_t *) entry;
	char *q = (char *) key;
	
	if (e->orig_qlen == keylen && memcmp(e->orig_q, q, keylen) == 0)
		return HT_SUCCESS;

	return HT_FAILURE;
}

int qcache_elem_free(void **entry)
{
	if (!entry)
		return HT_FAILURE;

	qcache_elem_t *qce = (qcache_elem_t *) *entry;
	if (!qce)
		return HT_FAILURE;

	free_qcache_elem(&qce);

	return HT_SUCCESS;
}

void
init_query_cache(query_cache_t *qcache, int size)
{
	init_hash_table(qcache, size, &qcache_ht_func);
}

void
reset_query_cache(query_cache_t *qcache)
{
	if (!qcache)
		return;

	list_t *idx_head, *idx_it, *idx_next;
	qcache_elem_t *qce;
	int i;
	for (i = 0; i < qcache->size; i++) {
		idx_head = &qcache->table[i];
		idx_it = idx_head->next;
		while (idx_it != idx_head) {
			qce = list_entry(idx_it, qcache_elem_t, table_listp);
			idx_next = idx_it->next;
			list_remove(idx_it);
			qcache->ht_func->hash_entry_free((void **) &qce);
			idx_it = idx_next;
		}
		list_init(idx_head);
	}

}

void
cleanup_query_cache(query_cache_t *qcache)
{
	if (!qcache)
		return;

	list_t *idx_head, *idx_it, *idx_next;
	qcache_elem_t *qce;
	int i;
	for (i = 0; i < qcache->size; i++) {
		idx_head = &qcache->table[i];
		idx_it = idx_head->next;
		while (idx_it != idx_head) {
			qce = list_entry(idx_it, qcache_elem_t, table_listp);
			idx_next = idx_it->next;
			list_remove(idx_it);
			qcache->ht_func->hash_entry_free((void **) &qce);
			idx_it = idx_next;
		}
	}

	free(qcache->table);
	memset(qcache, 0, sizeof(query_cache_t));
}

void
print_qcache_elem(qcache_elem_t *qce)
{
	if (!qce)
		return;

	qelem_t *e;
	list_t *rw_q_list = &qce->rw_q_list;

	printf("orig_str(%d): %s\n", qce->orig_qlen, qce->orig_q);
	if (list_empty(rw_q_list))
		printf("rw list: null\n");
	else {
		list_for_each_entry(e, rw_q_list, qsplit_listp) {
			qstr_print_split_elem(e);
		}
	}
}

void
print_query_cache(query_cache_t *qcache)
{
	if (!qcache)
		return;

	list_t *idx_head, *idx_it, *idx_next;
	qcache_elem_t *qce;
	int i;
	for (i = 0; i < qcache->size; i++) {
		idx_head = &qcache->table[i];
		if (list_empty(idx_head))
			continue;

		list_for_each_entry(qce, idx_head, table_listp) {
			print_qcache_elem(qce);
		}
	}
}

void
query_cache_add_entry(query_cache_t *qcache, qcache_elem_t *qce)
{
	char *key = qce->orig_q;
	int keylen = qce->orig_qlen;
	int idx = qcache->ht_func->hash_func(key, keylen, qcache->size);
#if DEBUG
	printf("INSERTED IDX: %d\n", idx);
#endif
	list_t *head = &qcache->table[idx];
	list_insert(head, &qce->table_listp);
}

void *
query_cache_get_entry(query_cache_t *qcache, void *key, int keylen, int exact)
{
	int idx = qcache->ht_func->hash_func(key, keylen, qcache->size);
#if DEBUG
	printf("LOOK FOR IDX: %d\n", idx);
#endif
	list_t *head = &qcache->table[idx];
	qcache_elem_t *qce;
	hash_fn_t *hfn = qcache->ht_func;
	list_for_each_entry(qce, head, table_listp) {
		if (hfn->hash_val_compare((void *) qce, key, keylen, exact) == HT_SUCCESS)
			return qce;
	}

	return NULL;
}

qcache_elem_t *
init_qcache_elem(char *orig_q, int orig_qlen, list_t *rw_q_list)
{
	qcache_elem_t *qce = (qcache_elem_t *) malloc(sizeof(qcache_elem_t));
	memset(qce, 0, sizeof(qcache_elem_t));
	list_init(&qce->rw_q_list);
	list_init(&qce->table_listp);
	set_qcache_elem_key(qce, orig_q, orig_qlen);
	set_qcache_elem_val(qce, rw_q_list);
	return qce;
}

void
free_qcache_elem(qcache_elem_t **qce_p)
{
	qcache_elem_t *qce = *qce_p;

	if (qce->orig_q)
		free(qce->orig_q);

	list_t *rw_head = &qce->rw_q_list;
	list_t *rw_it = rw_head->next;
	list_t *rw_next;
	qelem_t *e;
	while (rw_it != rw_head) {
		e = list_entry(rw_it, qelem_t, qsplit_listp);
		rw_next = rw_it->next;
		list_remove(rw_it);
		free_qelem(&e);
		rw_it = rw_next;
	}

	memset(qce, 0, sizeof(qcache_elem_t));
	list_init(&qce->rw_q_list);
	list_init(&qce->table_listp);

	free(qce);
	*qce_p = qce = NULL;
}

void
set_qcache_elem_key(qcache_elem_t *qce, char *orig_q, int orig_qlen)
{
	if (!qce || !orig_q || !orig_qlen)
		return;

	qce->orig_q = (char *) malloc(orig_qlen+1);
	memset(qce->orig_q, 0, orig_qlen+1);
	memcpy(qce->orig_q, orig_q, orig_qlen);
	qce->orig_qlen = orig_qlen;
}

void
set_qcache_elem_val(qcache_elem_t *qce, list_t *rw_q_list)
{
	if (!qce || !rw_q_list || list_empty(rw_q_list))
		return;

	list_migrate(&qce->rw_q_list, rw_q_list);
}

hash_fn_t qcache_ht_func = {
	murmur_hash,
	qcache_elem_compare,
	qcache_elem_free
};

