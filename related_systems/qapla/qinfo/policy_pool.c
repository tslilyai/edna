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

#include "utils/list.h"
#include "common/config.h"
#include "common/col_sym.h"
#include "common/qapla_policy.h"
#include "policy_pool.h"

typedef struct qapla_policy_pool_int {
	uint16_t max_pol;
	uint16_t next_pol_id;
	uint16_t num_pol;
	void **pol;
#if DEBUG
	list_t *pol_clist;
#endif
} qapla_policy_pool_int_t;

uint16_t 
get_next_pol_id(qapla_policy_pool_t *qpool)
{
	qapla_policy_pool_int_t *_qpool = (qapla_policy_pool_int_t *) qpool->priv;
	return _qpool->next_pol_id++;
}

qapla_policy_pool_t *
init_policy_pool(uint16_t max_pol)
{
	qapla_policy_pool_t *qpool = (qapla_policy_pool_t *) malloc(sizeof(qapla_policy_pool_t));
	qapla_policy_pool_int_t *_qpool = 
		(qapla_policy_pool_int_t *) malloc(sizeof(qapla_policy_pool_int_t));
	memset(_qpool, 0, sizeof(qapla_policy_pool_int_t));
	_qpool->max_pol = max_pol;
	_qpool->pol = (void **) malloc(sizeof(void *) * max_pol);
	memset(_qpool->pol, 0, sizeof(void *) * max_pol);
#if DEBUG
	_qpool->pol_clist = (list_t *) malloc(sizeof(list_t) * max_pol);
	int i;
	for (i = 0; i < max_pol; i++) {
		list_init(&_qpool->pol_clist[i]);
	}
#endif
	qpool->priv = (void *) _qpool;
	return qpool;
}

void 
free_policy_pool(qapla_policy_pool_t **pool)
{
	if (!pool || !*pool)
		return;

	int i;
	qapla_policy_pool_t *qpool = *pool;
	qapla_policy_pool_int_t *_qpool = (qapla_policy_pool_int_t *) qpool->priv;
	if (_qpool) {
		if (_qpool->pol) {
			for (i = 0; i < _qpool->max_pol; i++) {
				if (_qpool->pol[i]) {
					free(_qpool->pol[i]);
				}
			}

			free(_qpool->pol);
		}

#if DEBUG
		if (_qpool->pol_clist) {
			for (i = 0; i < _qpool->max_pol; i++) {
				cleanup_col_sym_list(&_qpool->pol_clist[i]);
			}
			free(_qpool->pol_clist);
		}
#endif

		free(_qpool);
		qpool->priv = NULL;
	}

	free(qpool);
	*pool = NULL;
}

void
print_policy_pool(qapla_policy_pool_t *qpool, FILE *f)
{
	if (!qpool || !qpool->priv || !f)
		return;

	int i;
	qapla_policy_pool_int_t *_qpool = (qapla_policy_pool_int_t *) qpool->priv;
	qapla_policy_t **ppol = (qapla_policy_t **) _qpool->pol;
	qapla_policy_t *qp;
	for (i = 0; i < _qpool->num_pol; i++) {
		qp = (qapla_policy_t *) ppol[i];
		fprintf(f, "%d. %s\n", i, qp->alias);
	}
}

void 
add_policy_to_pool(qapla_policy_pool_t *qpool, char *pol, int pol_len, list_t *clist)
{
	qapla_policy_pool_int_t *_qpool = (qapla_policy_pool_int_t *) qpool->priv;
	qapla_policy_t *qp = (qapla_policy_t *) pol;
	uint16_t pid = qapla_get_policy_id(qp);
	assert(!_qpool->pol[pid]);
	void *pool_elem = malloc(sizeof(qapla_policy_t));
	memcpy(pool_elem, qp, sizeof(qapla_policy_t));
	_qpool->pol[pid] = pool_elem;
#if DEBUG
	if (!list_empty(clist))
		list_migrate(&_qpool->pol_clist[pid], clist);
#endif
	_qpool->num_pol++;
}

char *
get_policy_from_pool(qapla_policy_pool_t *qpool, uint16_t pid)
{
	qapla_policy_pool_int_t *_qpool = (qapla_policy_pool_int_t *) qpool->priv;
	qapla_policy_t ** ppol = (qapla_policy_t **) _qpool->pol;
	return (char *) ppol[pid];
}

list_t *
get_policy_clist_from_pool(qapla_policy_pool_t *qpool, uint16_t pid)
{
#if DEBUG
	qapla_policy_pool_int_t *_qpool = (qapla_policy_pool_int_t *) qpool->priv;
	return &_qpool->pol_clist[pid];
#else
	return NULL;
#endif
}

uint16_t 
get_num_policy_in_pool(qapla_policy_pool_t *qpool)
{
	qapla_policy_pool_int_t *_qpool = (qapla_policy_pool_int_t *) qpool->priv;
	return _qpool->num_pol;
}
