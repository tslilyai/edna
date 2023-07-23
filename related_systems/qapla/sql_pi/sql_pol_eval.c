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

#include "common/qapla_policy.h"
#include "common/tuple.h"
#include "common/dlog_pi_ds.h"
#include "common/dlog_pi_tools.h"
#include "common/col_sym.h"
#include "dlog_pi/dlog_pi_env.h"
#include "sql_pol_eval.h"
#include "utils/list.h"
#include "qinfo/query_info_int.h"

typedef struct placeholder {
	char *pos;
	int len;
	int resolve_type;
	char *resolve_pos;
	int resolve_len;
	uint16_t varId;
	list_t plc_listp;
} placeholder_t;

placeholder_t *
init_placeholder(void)
{
	placeholder_t *plc = (placeholder_t *) malloc(sizeof(placeholder_t));
	memset(plc, 0, sizeof(placeholder_t));
	list_init(&plc->plc_listp);
	return plc;
}

void
cleanup_placeholder_list(list_t *plc_list)
{
	list_t *list_plc_it = plc_list->next;
	list_t *list_next_plc_it;
	placeholder_t *plc_it = NULL;
	while (list_plc_it != plc_list) {
		list_next_plc_it = list_plc_it->next;
		plc_it = list_entry(list_plc_it, placeholder_t, plc_listp);
		list_remove(list_plc_it);
		free(plc_it);
		list_plc_it = list_next_plc_it;
	}
}

int16_t
get_var_id_from_text(char *str, int len)
{
	uint16_t varId = 0, digit;
	int i, mask = 1;
	for (i = 0; i < len; i++) {
		digit = str[i] - '0';
		if (!(digit >= 0 && digit <= 9))
			return -1;

		varId = varId * 10 + digit;
	}

	return varId;
}

void
sql_copy_param_value(char *ptr, placeholder_t *plc)
{
	if (plc->resolve_type == TTYPE_VARLEN) {
		//memcpy(ptr, "'", 1);
		//ptr += 1;
		memcpy(ptr, plc->resolve_pos, plc->resolve_len);
		ptr += plc->resolve_len;
		//memcpy(ptr, "'", 1);
	} else {
		memcpy(ptr, plc->resolve_pos, plc->resolve_len);
	}
}

void
sql_populate_plc_value(placeholder_t *plc, dlog_pi_env_t *env)
{
	dlog_tuple_t *dlog_var;
	uint8_t ret;
	ret = dlog_pi_env_getVar(env, plc->varId, (uint8_t **) &dlog_var);
	plc->resolve_type = TUP_ELEMENT_TYPE(*dlog_var, 0);
	if (TUP_ELEMENT_TYPE(*dlog_var, 0) == TTYPE_VARLEN) {
		// strlen(var) + strlen('')
		plc->resolve_len = get_var_length_ptr_len(dlog_var, 0) - 1; 
		plc->resolve_pos = (char *) get_var_length_ptr(dlog_var, 0);
	} else {
		plc->resolve_len = TUP_ELEMENT_TYPE_SIZE(*dlog_var, 0);
		plc->resolve_pos = (char *) TUP_PTR_ELEMENT(dlog_var, 0);
	}
}

int
match_pred_tid_list(dlog_pi_op_t *op, list_t *tlist)
{
	uint64_t tid = *(uint64_t *) TUP_PTR_ELEMENT(&op->tuple, 0);
	symbol_t *sym;
	col_sym_t *cs;
	list_t *cs_head;
	list_t *sym_head = tlist;
	list_for_each_entry(sym, sym_head, symbol_listp) {
		if (sym_field(sym, db_tid) == tid)
			return 1;

		cs_head = &sym->col_sym_list;
		list_for_each_entry(cs, cs_head, col_sym_listp) {
			if (sym_field(cs, db_tid) == tid)
				return 1;
		}
	}

	return 0;
}

uint8_t
sql_parse_params(qapla_policy_t *qp, qapla_perm_id_t perm, int clause_idx, 
		dlog_pi_env_t *env, qapla_policy_t *out_qp, list_t *tlist)
{
	if (!qp || perm >= QP_NUM_PERMS || !env)
		return SQL_PI_FAILURE;

	if (check_allow_all_clause(qp, perm, CTYPE_SQL, clause_idx))
		return SQL_PI_GRANTED_PERM;

	if (check_disallow_all_clause(qp, perm, CTYPE_SQL, clause_idx))
		return SQL_PI_DISALLOWED;

	qapla_set_policy_id(out_qp, qapla_get_policy_id(qp));
	qapla_set_perm_clauses(out_qp, perm, 1);
	char *out_perm = qapla_start_perm(out_qp, perm);
	char *out_perm_sql_op = qapla_start_perm_clause(out_qp, perm, CTYPE_SQL, 0);
	dlog_pi_op_t *out_sql_op = (dlog_pi_op_t *) out_perm_sql_op;

	char *sql_op_start = qapla_get_perm_clause_start(qp, perm, CTYPE_SQL, clause_idx);
	char *sql_op_end = qapla_get_perm_clause_end(qp, perm, CTYPE_SQL, clause_idx);
	dlog_pi_op_t *op = (dlog_pi_op_t *) sql_op_start;

	list_t plc_list;
	list_init(&plc_list);
	placeholder_t *plc;

	char citr, *startc, *endc;
	int i = 0, j = 0, varc = 0, reset = 0, invalid_var = 0;
	char *newquery;
	int newquery_len = 0;

	for (; (char *) op < sql_op_end; op = next_operation(op)) {
		
		if (match_pred_tid_list(op, tlist) == 0)
			continue;

		char *sql_op = get_var_length_ptr(&op->tuple, 1);
		int qlen = get_var_length_ptr_len(&op->tuple, 1) - 1;
		char *sql_qend = sql_op + qlen;
		varc = get_policy_parameters(sql_op, qlen, &plc_list);

		if (varc <= 0) {
			dlog_pi_op_t *tmp_next_op = next_operation(op);
			int op_len = ((char *) tmp_next_op - (char *) op);
			memcpy((char *) out_sql_op, (char *) op, op_len);
			out_sql_op = (dlog_pi_op_t *) ((char *) out_sql_op + op_len);
		} else {
			fill_policy_parameters(sql_op, qlen, &plc_list, &newquery, &newquery_len,
					NULL, env);

			uint64_t tid = *(uint64_t *) TUP_PTR_ELEMENT(&op->tuple, 0);
			out_sql_op = create_n_arg_cmd(out_sql_op, DLOG_P_SQL, 2, TTYPE_INTEGER, &tid,
					TTYPE_VARLEN, newquery, newquery+newquery_len+1);

			cleanup_placeholder_list(&plc_list);
			free(newquery);
		}
	}

	qapla_end_perm_clause(out_qp, perm, CTYPE_SQL, 0, (char *) out_sql_op);
	qapla_end_perm(out_qp, perm, (char *) out_sql_op);

	return SQL_PI_SUCCESS;
}

int
match_pred_tid(dlog_pi_op_t *op, uint64_t tid)
{
	uint64_t pol_tid = *(uint64_t *) TUP_PTR_ELEMENT(&op->tuple, 0);
	if (pol_tid == tid)
		return 1;

	return 0;
}

int
match_sql_table_in_pol(qapla_policy_t *qp, qapla_perm_id_t perm, uint64_t tid)
{
	if (!qp || perm >= QP_NUM_PERMS)
		return SQL_PI_FAILURE;

	int i;
	int num_clause = qapla_get_num_perm_clauses(qp, perm);
	char *clause_start, *clause_end;
	dlog_pi_op_t *sql_op;
	// It should be enough to check the first non-zero sql clause for tid.
	// all clauses in a policy must consistently refer to all tables
	for (i = 0; i < num_clause; i++) {
		if (check_allow_all_clause(qp, perm, CTYPE_SQL, i))
			return SQL_PI_GRANTED_PERM;

		if (check_disallow_all_clause(qp, perm, CTYPE_SQL, i))
			return SQL_PI_DISALLOWED;

		clause_start = qapla_get_perm_clause_start(qp, perm, CTYPE_SQL, i);
		clause_end = qapla_get_perm_clause_end(qp, perm, CTYPE_SQL, i);
		if (clause_end <= clause_start || clause_start <= (char *) qp)
			continue;

		sql_op = (dlog_pi_op_t *) clause_start;
		if (match_pred_tid(sql_op, tid))
			return 1;
		else
			return 0;
	}

	return 0;
}

int
get_policy_parameters(char *q, int qlen, list_t *plc_list)
{
	char citr, *startc = NULL, *endc = NULL, *start_ptr = NULL, *end_ptr = NULL;
	int i = 0, j = 0, varc = 0, reset = 0, invalid_var = 0;
	placeholder_t *plc = NULL;

	start_ptr = q;
	end_ptr = q + qlen;
	varc = 0;
	for (i = 0; i < qlen; i++) {
		citr = start_ptr[i];
		if (citr != ':')
			continue;

		startc = &start_ptr[i+1];
		for (j = i+1; ; j++) {
			citr = start_ptr[j];
			switch (citr) {
				case '\\':
				case '\'': // e.g. =':var'
				case ' ': // e.g. =':var '
				case '"': // e.g. = ":var"
				case ')': // e.g. =:var)
				case '%': // e.g. like '%:var%'
					{
						endc = &start_ptr[j];
						int16_t varId;
						int var_len = endc - startc;
						varId = get_var_id_from_text(startc, var_len);
						if (varId >= 0) {
							// after ':'
							plc = init_placeholder();
							plc->pos = &start_ptr[i+1];
							plc->varId = varId;
							plc->len = var_len;
							list_insert_at_tail(plc_list, &plc->plc_listp);
							reset = 1;
						} else {
							invalid_var = 1;
						}
					}
					break;
			}
			if (invalid_var) {
				i = j;
				invalid_var = 0;
				break;
			}
			if (reset) {
				i = j;
				varc++;
				reset = 0;
				break;
			}
		}
	}

	if (varc <= 0)
		return -1;

	return varc;
}

void
fill_policy_parameters(char *q, int qlen, list_t *plc_list,
		char **new_q, int *new_qlen, char *email, dlog_pi_env_t *env)
{
	int varc = 0, fragc = 0;
	placeholder_t *plc_it, *plc_next_it, *plc_prev_it;
	list_for_each_entry(plc_it, plc_list, plc_listp) {
		varc++;
	}

	int nfrag = varc + 1;
	int *frag_len = (int *) malloc(sizeof(int) * nfrag);
	char *newquery = NULL;
	int newquery_len = 0;
	memset(frag_len, 0, sizeof(int) * nfrag);

	char *start_ptr = q;
	char *end_ptr = q + qlen;
	list_for_each_entry(plc_it, plc_list, plc_listp) {
		if (fragc == 0) {
			frag_len[fragc] = plc_it->pos - 1 - start_ptr;
			newquery_len += frag_len[fragc++];
		} else {
			frag_len[fragc] = plc_it->pos - 1 - (plc_prev_it->pos + plc_prev_it->len);
			newquery_len += frag_len[fragc++];
		}
#if CONFIG_REFMON != RM_CACHE_FULL_POLICY
		//
		if (plc_it->varId == 0) {
			plc_it->resolve_type == TTYPE_VARLEN;
			plc_it->resolve_pos = email;
			plc_it->resolve_len = strlen(plc_it->resolve_pos);
		} else {
			// right now we have only one variable type in the policies
			assert(0);
		}
#else
		sql_populate_plc_value(plc_it, env);
#endif
		newquery_len += plc_it->resolve_len;
		plc_prev_it = plc_it;
		if (fragc == varc)
			break;
	}

	frag_len[fragc] = end_ptr - (plc_it->pos + plc_it->len);
	newquery_len += frag_len[fragc++];

	fragc = 0;
	newquery = (char *) malloc(newquery_len + 1);
	memset(newquery, 0, newquery_len + 1);
	char *itr = newquery;
	list_for_each_entry(plc_it, plc_list, plc_listp) {
		if (fragc == 0) {
			memcpy(itr, start_ptr, frag_len[fragc]);
			itr += frag_len[fragc++];
		} else {
			memcpy(itr, (plc_prev_it->pos + plc_prev_it->len), frag_len[fragc]);
			itr += frag_len[fragc++];
		}
		sql_copy_param_value(itr, plc_it);
		itr += plc_it->resolve_len;

		plc_prev_it = plc_it;
	}
	memcpy(itr, (plc_prev_it->pos + plc_prev_it->len), frag_len[fragc]);
	itr += frag_len[fragc++];

	*new_q = newquery;
	*new_qlen = newquery_len;
	free(frag_len);
}

