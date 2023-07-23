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
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "utils/list.h"
#include "common/sym_xform.h"
#include "common/col_sym.h"
#include "query_symbol.h"
#include "pol_eval_utils.h"

int
get_distinct_table_array(uint64_t *tab_arr, int *num_tab, int max_tab, list_t *t_list)
{
	int n_tab = 0;
	uint64_t tid;
	symbol_t *sym;
	col_sym_t *tab_cs;
	list_t *tab_cs_list;
	list_t *tab_list = t_list;
	list_for_each_entry(sym, tab_list, symbol_listp) {
		tab_cs_list = &sym->col_sym_list;
		list_for_each_entry(tab_cs, tab_cs_list, col_sym_listp) {
			tid = (uint64_t) sym_field(tab_cs, db_tid);
			
			if ((int64_t) tid < 0 || tid == DUAL_TID) // temporary tables
				continue;

			if (get_table_index(tab_arr, n_tab, tid) >= 0)
				continue;
			tab_arr[n_tab] = tid;
			n_tab++;
			if (n_tab >= max_tab) {
				fprintf(stderr, "ERROR: query accesses more than %d tables!!\n", max_tab);
				return -1;
			}
		}
	}

	*num_tab = n_tab;
	return 0;
}

int
get_table_index(uint64_t *t_arr, int num_tab, uint64_t tid)
{
	int i;
	for (i = 0; i < num_tab; i++) {
		if (t_arr[i] == tid)
			return i;
	}

	return -1;
}

symbol_t *
get_table_sym(list_t *tab_list, uint64_t tid)
{
	list_t *t_it = tab_list;
	symbol_t *tsym;
	uint64_t ttid;
	list_for_each_entry(tsym, t_it, symbol_listp) {
		ttid = sym_field(tsym, db_tid);
		if (ttid == tid)
			return tsym;
	}

	return NULL;
}

void
print_table_col_list(list_t *tab_list, FILE *f)
{
	list_t *t_it = tab_list;
	list_t *c_it;
	symbol_t *tsym;
	col_sym_t *ccs;
	char buf[512];
	char col[512];
	memset(buf, 0, 512);
	memset(col, 0, 512);
	list_for_each_entry(tsym, t_it, symbol_listp) {
		c_it = &tsym->col_sym_list;
		fprintf(f, "%s(%d) => ", sym_field(tsym, db_tname), sym_field(tsym, db_tid));
		list_for_each_entry(ccs, c_it, col_sym_listp) {
			sprintf(col, "%s(%d)", sym_field(ccs, db_cname), sym_field(ccs, db_cid));
			get_xform_list(buf, NULL, &ccs->xform_list, col);
			fprintf(f, "%s, ", buf);
		}
		fprintf(f, "null\n");
	}
}

