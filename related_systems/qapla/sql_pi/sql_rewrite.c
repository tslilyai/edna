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
#include <assert.h>

#include "utils/list.h"
#include "common/config.h"
#include "common/qapla_policy.h"
#include "common/query.h"
#include "common/dlog_pi_ds.h"
#include "common/dlog_pi_tools.h"
#include "qinfo/query_info_int.h"

#include "sql_rewrite.h"
#include "sql_pol_eval.h"

dlog_pi_op_t *
get_match_tid_pred(qpos_t *pos, qapla_policy_t *qp)
{
	char *sql_op = qapla_get_perm_clause_start(qp, QP_PERM_READ, CTYPE_SQL, 0);
	char *sql_op_end = qapla_get_perm_clause_end(qp, QP_PERM_READ, CTYPE_SQL, 0);
	dlog_pi_op_t *op = (dlog_pi_op_t *) sql_op;

	for (; (char *) op < sql_op_end; op = next_operation(op)) {
		uint64_t tid = *(uint64_t *) TUP_PTR_ELEMENT(&op->tuple, 0);
		uint64_t qpos_tid = (uint64_t) sym_ptr_field(pos, db_tid);
		if (tid == qpos_tid)
			return op;
	}

	return NULL;
}

int
get_match_tid_pred_len(qpos_t *pos, qapla_policy_t *qp)
{
	dlog_pi_op_t *op = get_match_tid_pred(pos, qp);
	if (!op)
		return 0;
	int len = get_pred_sql_len(op);
#if DEBUG
	printf("tid: %d, table: %s, pred len: %d\n", sym_ptr_field(pos, db_tid),
			sym_ptr_field(pos, db_tname), len);
#endif
	return len;
}

char *
get_match_tid_pred_sql(qpos_t *pos, qapla_policy_t *qp, int *sql_len, int *db_tid)
{
	dlog_pi_op_t *op = get_match_tid_pred(pos, qp);
	if (!op)
		return NULL;
	char *sql = get_pred_sql(op);
	*sql_len = get_pred_sql_len(op);
	*db_tid = sym_ptr_field(pos, db_tid);
	return sql;
}

int
compute_frag_len(char *orig_q, qpos_t *curr, qpos_t *prev)
{
	// for first frag, always copy till start off of table name
	if (!prev)
		return curr->off;

	if (prev->alias_off && prev->alias_len)
		return curr->off - prev->alias_off;

	return curr->off - prev->off;
}

void
copy_frag(char *new_q, qpos_t *curr, qpos_t *prev, char *orig_q, int len)
{
	if (!prev) {
		memcpy(new_q, orig_q, len);
		return;
	}

	if (prev->alias_off && prev->alias_len) {
		memcpy(new_q, prev->alias_pos, len);
		return;
	}

	memcpy(new_q, prev->pos, len);
}

static int8_t
match_tid_index(qpos_t *pos, list_t *table_list)
{
	int idx = 0;
	list_t *t_it = table_list;
	symbol_t *tsym;
	int q_tid, t_tid;
	list_for_each_entry(tsym, t_it, symbol_listp) {
		q_tid = sym_ptr_field(pos, db_tid);
		t_tid = sym_field(tsym, db_tid);
		if (q_tid == t_tid)
			return idx;

		idx++;
	}

	return -1;
}

static int8_t
match_tname_index(char *tname, list_t *table_list) {
	int idx = 0;
	char *t_tname;
	symbol_t *tsym;
	list_for_each_entry(tsym, table_list, symbol_listp) {
		t_tname = sym_field(tsym, db_tname);
		if ((strlen(t_tname) == strlen(tname)) && 
				strncmp(tname, t_tname, strlen(tname)) == 0)
			return idx;

		idx++;
	}

	return -1;
}

uint8_t
sql_rewrite(query_info_int_t *qi, list_t *table_list, char **tab_sql, int *tab_sql_len,
		int n_tab_sql)
{
	char *orig_q = NULL, *new_q = NULL;
	char *ptr = NULL;
	int orig_qlen = 0, new_qlen = 0;
	int num_qpos = 0;
	int match_tname_idx = 0;
	list_t *qsplit_list;
	qelem_t *e;

	qsplit_list = qstr_get_split_list(&qi->qstr);
	list_for_each_entry(e, qsplit_list, qsplit_listp) {
		switch (e->elem_type) {
			case QE_POLICY:
				match_tname_idx = match_tname_index(e->orig_str, table_list);
				assert(match_tname_idx >= 0 && match_tname_idx <= n_tab_sql);
				assert(!e->new_str);
				// +2 for (), +1 for space after policy string
				e->new_len = tab_sql_len[match_tname_idx]-1 + 2 + 1; 
				e->new_str = (char *) malloc(e->new_len+1);
				memset(e->new_str, 0, e->new_len+1);
				ptr = e->new_str;
				memcpy(ptr, "(", 1);
				ptr += 1;
				memcpy(ptr, tab_sql[match_tname_idx], tab_sql_len[match_tname_idx]-1);
				ptr += tab_sql_len[match_tname_idx]-1;
				memcpy(ptr, ")", 1);
				ptr += 1;
				memcpy(ptr, " ", 1);
				ptr += 1;

				new_qlen += e->new_len;
				if (e->alias_str && e->alias_len)
					new_qlen += e->alias_len;
				else
					new_qlen += e->orig_len;
				break;

			case QE_DEFAULT:
			case QE_SYNTAX:
			case QE_PLC:
				new_qlen += e->orig_len;
				break;

			default: assert(0);
		}
	}

	return SQL_PI_SUCCESS;
}

void
resolve_policy_parameters(query_info_int_t *qi, list_t *qsplit_list)
{
	qelem_t *e = NULL;

	list_t plc_list;
	list_init(&plc_list);
	char *new_q = NULL;
	int new_qlen = 0;

	list_for_each_entry(e, qsplit_list, qsplit_listp) {
		if (e->elem_type != QE_POLICY)
			continue;

		int varc = get_policy_parameters(e->new_str, e->new_len, &plc_list);
		if (varc <= 0)
			continue;

		fill_policy_parameters(e->new_str, e->new_len, &plc_list, &new_q, &new_qlen, get_session_email(&qi->session), NULL);
		if (new_q && new_qlen) {
			free(e->new_str);
			e->new_str = new_q;
			e->new_len = new_qlen;
		}
		cleanup_placeholder_list(&plc_list);
	}
}
