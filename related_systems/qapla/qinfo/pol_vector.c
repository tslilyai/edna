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

#include "pol_vector.h"
#include "common/config.h"
#include "common/db.h"

typedef struct pvec_int {
	hash_table_t *pvec_index;
	pvec_fn_t *pfn;
} pvec_int_t;

int16_t
get_pid_from_pvec(PVEC pv)
{
	int i, one_pid = 0;
	int16_t pid;
	int is_set;
	for (i = 0; i < MAX_POLICIES; i++) {
		is_set = ((int) (pv >> i) & 1);
		if (!is_set)
			continue;

		if (!one_pid) {
			pid = (int16_t) i;
			one_pid = 1;
		} else {
			return (int16_t) -1;
		}
	}

	return pid;
}

void
init_pvec_index(pvec_t *pv, pvec_fn_t *pfn, int index_size)
{
	if (!pv)
		return;

	pvec_int_t *pv_int = (pvec_int_t *) malloc(sizeof(pvec_int_t));
	pv_int->pfn = pfn;
	pv_int->pvec_index = pfn->init_pol_vector(index_size);
	pv->priv = (void *) pv_int;
}

void
init_schema_pvec(pvec_t *pv, pvec_fn_t *pfn, db_t *schema)
{
	if (!pv)
		return;

	pvec_int_t *pv_int = (pvec_int_t *) malloc(sizeof(pvec_int_t));
	pv_int->pfn = pfn;
	pv_int->pvec_index = pfn->generate_schema_pol_vector(schema);
	pv->priv = (void *) pv_int;
}

pvec_t *
alloc_init_pvec_index(pvec_fn_t *pfn, db_t *schema)
{
	pvec_t *pv = (pvec_t *) malloc(sizeof(pvec_t));
	init_schema_pvec(pv, pfn, schema);
	return pv;
}

void
cleanup_pvec_index(pvec_t *pv)
{
	if (!pv || !pv->priv)
		return;

	pvec_int_t *pv_int = (pvec_int_t *) pv->priv;
	pvec_fn_t *pfn = pv_int->pfn;
	pfn->free_schema_pol_vector(&pv_int->pvec_index);
	pv_int->pfn = NULL;

	free(pv_int);
	pv->priv = NULL;
}

void
free_pvec_index(pvec_t **pv)
{
	if (!pv || !(*pv))
		return;

	cleanup_pvec_index(*pv);
	free(*pv);
	*pv = NULL;

	return;
}

void
add_policy_to_pvec_index(pvec_t *pv, uint16_t pid, list_t *clist)
{
	pvec_int_t *pv_int = (pvec_int_t *) pv->priv;
	pvec_fn_t *pfn = pv_int->pfn;
	pfn->add_policy_to_col_list(pv_int->pvec_index, pid, clist);
	return;
}

void
add_col_policy_to_pvec_index(pvec_t *pv, uint16_t pid, col_sym_t *cs)
{
	pvec_int_t *pv_int = (pvec_int_t *) pv->priv;
	pvec_fn_t *pfn = pv_int->pfn;
	pfn->add_policy_to_col(pv_int->pvec_index, pid, cs);
	return;
}

PVEC
get_policy_from_pvec_index(pvec_t *pv, list_t *clist)
{
	pvec_int_t *pv_int = (pvec_int_t *) pv->priv;
	pvec_fn_t *pfn = pv_int->pfn;
	PVEC pvec = pfn->compute_query_pol_vector(pv_int->pvec_index, clist);
	return pvec;
}

PVEC
get_column_pvec(pvec_t *pv, col_sym_t *cs)
{
	pvec_int_t *pv_int = (pvec_int_t *) pv->priv;
	pvec_fn_t *pfn = pv_int->pfn;
	PVEC pvec = pfn->get_col_pol_vector(pv_int->pvec_index, cs);
	return pvec;
}

void
print_pvec_index(pvec_t *pv, FILE *f)
{
	pvec_int_t *pv_int = (pvec_int_t *) pv->priv;
	pvec_fn_t *pfn = pv_int->pfn;
	pfn->print_schema_pol_vector(pv_int->pvec_index, f);
	return;
}
