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

#include "common/sym.h"
#include "pol_eval_mv.h"
#include "pol_eval.h"
#include "pol_eval_utils.h"

const char *mv_sql_check = (const char *) "IS NOT NULL";

int
query_get_table_list(query_info_int_t *qi, uint64_t *tid_arr, int max_tab, int *n_tab)
{
	if (!qi || !tid_arr || !max_tab || !n_tab)
		return POL_EVAL_ERROR;

	parser_context_t *pc = &qi->pc;
	context_t *firstC = get_first_context(pc);
	if (!firstC)
		return POL_EVAL_ERROR;

	list_t *t_list = &firstC->symbol_list[SYM_TAB];

	int ret = get_distinct_table_array(tid_arr, n_tab, max_tab, t_list);
	return (ret < 0 ? POL_EVAL_ERROR : POL_EVAL_SUCCESS);
}

int
query_segregate_cols_to_tables(query_info_int_t *qi, list_t *table_list)
{
	if (!qi || !table_list)
		return POL_EVAL_ERROR;

	parser_context_t *pc = &qi->pc;
	context_t *firstC = get_first_context(pc);
	if (!firstC)
		return POL_EVAL_ERROR;

	list_t *c_list = &firstC->symbol_list[SYM_COL];
	list_t *c_it = NULL;
	symbol_t *csym = NULL;
	col_sym_t *ccs = NULL, *new_ccs = NULL, *inserted = NULL;

	symbol_t *tsym = NULL;
	uint64_t tid, cid;

	list_for_each_entry(csym, c_list, symbol_listp) {
		c_it = &csym->col_sym_list;
		list_for_each_entry(ccs, c_it, col_sym_listp) {
			tid = sym_field(ccs, db_tid);
			cid = sym_field(ccs, db_cid);
			if ((int) tid < 0 || (int) cid < 0 || (int) tid == DUAL_TID || (int) cid == SPECIAL_CID)
				continue;

			tsym = get_table_sym(table_list, tid);
			new_ccs = dup_col_sym(ccs);
			inserted = insert_col_sym_to_sym_list_fn(tsym, new_ccs, CHECK_ONLY_CID);
			if (inserted != new_ccs) {
				free_col_sym(&new_ccs);
				new_ccs = inserted;
			}
			//list_insert(&tsym->col_sym_list, &new_ccs->col_sym_listp);
		}
	}

	return POL_EVAL_SUCCESS;
}

int
get_mv_sql_for_table(query_info_int_t *qi, symbol_t *tsym, char **sql, int *sql_len)
{
	if (!qi || !tsym || !sql || !sql_len)
		return POL_EVAL_ERROR;

	col_sym_t *ccs = NULL;
	list_t *c_list = &tsym->col_sym_list;
	int ccs_str_len = 0;
	char *ccs_cname = NULL;
	int first = 1;

	char *select_str = "select * from ";
	char *table_str = sym_field(tsym, db_tname);
	char *where_str = " where ";

	ccs_str_len += strlen(select_str);
	ccs_str_len += strlen(table_str);
	ccs_str_len += strlen(where_str);
	//ccs_str_len += strlen(table_str);

	list_for_each_entry(ccs, c_list, col_sym_listp) {
		ccs_cname = sym_field(ccs, db_cname);
		if (!first) {
			ccs_str_len += 5; // for " and "
		} else {
			first = 0;
		}

#if DEBUG
		printf("<%d.%d> %s:%s\n", sym_field(ccs, db_tid), sym_field(ccs, db_cid),
				sym_field(ccs, db_tname), sym_field(ccs, db_cname));
#endif
		ccs_str_len += strlen(ccs_cname) + 1; // +1 for space after cname in "cname != NULL"
		ccs_str_len += strlen(mv_sql_check);
	}
	ccs_str_len += 1; // for NULL character
#if DEBUG
	printf("ccsstr_len: %d\n", ccs_str_len);
#endif

	char *ccs_sql = (char *) malloc(ccs_str_len);
	memset(ccs_sql, 0, ccs_str_len);

	char *ptr = ccs_sql;
	int off = 0;
	first = 1;

	sprintf(ptr + off, "%s%s%s", select_str, table_str, where_str);
	off = strlen(ptr);
	list_for_each_entry(ccs, c_list, col_sym_listp) {
		if (!first) {
			sprintf(ptr + off, " and ");
			off = strlen(ptr);
		} else {
			first = 0;
		}

		ccs_cname = sym_field(ccs, db_cname);
		sprintf(ptr + off, "%s %s", ccs_cname, mv_sql_check);
		off = strlen(ptr);
	}
	//sprintf(ptr + off, "%s%s", end_str, table_str);
	//off = strlen(ptr);

#if DEBUG
	printf("len: %d, %d, %d, %s\n", ccs_str_len, off, strlen(ccs_sql), ccs_sql);
#endif

	*sql = ccs_sql;
	*sql_len = ccs_str_len;
}
