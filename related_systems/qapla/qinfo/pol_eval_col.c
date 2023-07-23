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
#include "utils/hashtable.h"
#include "utils/format.h"
#include "common/config.h"
#include "common/session.h"
#include "common/qapla_policy.h"
#include "common/tuple.h"
#include "common/db.h"

#include "policyapi/dlog_pred.h"
#include "policyapi/sql_pred.h"

#include "dlog_pi/dlog_pi_env.h"
#include "dlog_pi/dlog_pi.h"

#include "sql_pi/sql_pol_eval.h"
#include "sql_pi/sql_rewrite.h"

#include "query_info_int.h"
#include "metadata.h"
#include "pol_cluster.h"
#include "pol_vector.h"

#include "pol_eval_utils.h"
#include "pol_eval_col.h"
#include "pol_eval.h"

#include <antlr3.h>
#include <glib.h>

int
query_get_pid_for_each_col(query_info_int_t *qi, list_t *table_list, 
		qapla_policy_t **qp_list, int *n_qp)
{
	if (!qi || !table_list || !qp_list)
		return POL_EVAL_ERROR;

	parser_context_t *pc = &qi->pc;
	context_t *firstC = get_first_context(pc);
	if (!firstC)
		return POL_EVAL_ERROR;

	list_t *t_list = &firstC->symbol_list[SYM_TAB];
	list_t *col_list = &firstC->symbol_list[SYM_COL];

	list_t *t_it = table_list;
	list_t *c_it;
	symbol_t *tsym, *csym, *new_tsym, *new_csym;
	col_sym_t *ccs, *new_ccs, *match_ccs;
	char *tname, *cname;
	uint64_t tid, cid;

	int i, ret;
	uint64_t tab_arr[10];
	int n_tab, max_tab = 10;
	memset(tab_arr, 0, sizeof(uint64_t) * max_tab);
	ret = get_distinct_table_array(tab_arr, &n_tab, max_tab, t_list);

	for (i = 0; i < n_tab; i++) {
		tid = tab_arr[i];
		if ((int) tid < 0)
			continue; // for temporary tables

		tname = get_schema_table_name(&qi->schema, (int) tid);

		new_tsym = (symbol_t *) malloc(sizeof(symbol_t));
		init_symbol(new_tsym);
		set_sym_str_field(new_tsym, db_tname, tname);
		set_sym_field(new_tsym, db_tid, tid);
		list_insert(table_list, &new_tsym->symbol_listp);
	}

	metadata_t *qm = &qi->qm;
	int n_cluster = metadata_get_num_clusters(qm);
	PVEC *c_pvec = (PVEC *) malloc(sizeof(PVEC) * n_cluster);
	uint8_t *c_used = (uint8_t *) malloc(sizeof(uint8_t) * n_cluster);
	uint8_t *c_type = (uint8_t *) malloc(sizeof(uint8_t) * n_cluster);
	cluster_t **cluster = (cluster_t **) malloc(sizeof(cluster_t *) * n_cluster);
	memset(c_pvec, 0, sizeof(PVEC) * n_cluster);
	memset(c_used, 0, sizeof(uint8_t) * n_cluster);
	memset(c_type, 0, sizeof(uint8_t) * n_cluster);
	memset(cluster, 0, sizeof(cluster_t *) * n_cluster);

	cluster_t *tmp_c;
	PVEC g_tmp_pv = 0, log_tmp_pv = 0, final_pv = -1;
	int match_ret, fail;
	int cluster_id, cluster_op;
	int16_t pid = -1;
	qapla_policy_t *qp = NULL;

	int n_distinct_pol = 0;

	list_for_each_entry(csym, col_list, symbol_listp) {
		c_it = &csym->col_sym_list;
		list_for_each_entry(ccs, c_it, col_sym_listp) {
			// add col to table list, and set policy pointer (one symbol per col)
			tid = sym_field(ccs, db_tid);
			cid = sym_field(ccs, db_cid);	
			if ((int) tid < 0 || (int) cid < 0 || (int) tid == DUAL_TID ||
					(int) cid == SPECIAL_CID) // temporary tables
				continue;
			tsym = get_table_sym(table_list, tid);
			match_ret = exists_col_sym_in_list(ccs, &tsym->col_sym_list, 0, &match_ccs);

			g_tmp_pv = 0;
			ret = metadata_lookup_cluster_pvec(qm, ccs, &tmp_c, &g_tmp_pv);
			if (ret < 0) {
				assert(0);
			} else {
				cluster_id = cluster_get_id(tmp_c);
				cluster_op = cluster_get_op_type(tmp_c);
				c_type[cluster_id] = cluster_op;
				if (cluster_op == OP_OR) {
					pid = get_pid_from_pvec(g_tmp_pv);
					if (pid < 0) {
						fprintf(stderr, "too many policies on independent column %s(%d):0x%lx\n",
								sym_field(ccs, db_cname), sym_field(ccs, db_cid), g_tmp_pv);
						assert(0);
					} else {
						qp = (qapla_policy_t *) get_pol_for_pid(qm, pid);
						if (!qp_list[pid])
							n_distinct_pol++;

						qp_list[pid] = qp;

						if (match_ret == 0) {
							if (match_ccs->ptr == (void *) qp)
								continue;
							else // multiple policies on same col in independent cluster
								assert(0);
						}
						new_ccs = dup_col_sym(ccs);
						list_insert(&tsym->col_sym_list, &new_ccs->col_sym_listp);
						new_ccs->ptr = (void *) qp;
					}
				} else {
					// link policies
					if (!c_used[cluster_id]) {
						cluster[cluster_id] = tmp_c;
						c_pvec[cluster_id] = -1;
						c_used[cluster_id] = 1;
					}
					c_pvec[cluster_id] &= g_tmp_pv;
					if (c_pvec[cluster_id] == 0)
						error_print_col_link(qi->log_info.f, ccs);

					if (match_ret == 0) {
						if (match_ccs->ptr == (void *) tmp_c)
							continue;
						else // column exists in multiple clusters
							assert(0);
					}
					new_ccs = dup_col_sym(ccs);
					list_insert(&tsym->col_sym_list, &new_ccs->col_sym_listp);
					new_ccs->ptr = (void *) tmp_c;
				}
			}
		}
	}

	int16_t *least_pid = (int16_t *) malloc(sizeof(int16_t) * n_cluster);
	PVEC tmp_pv = 0;
	for (i = 0; i < n_cluster; i++) {
		if (!cluster[i] || c_type[i] == OP_OR)
			continue;

		if (c_pvec[i] == 0)
			fprintf(qi->log_info.f, "ERROR!! link policy is false, cluster: %d\n", i);

		least_pid[i] = get_least_restrictive_pid(cluster[i], c_pvec[i]);
		// 0xffffffffffffffff
		if (least_pid[i] < 0)
			continue;

#if DEBUG
		printf("%d. least pid: %d\n", i, least_pid[i]);
#endif
		tmp_pv |= ((PVEC) 1 << least_pid[i]);
		c_pvec[i] = tmp_pv;
	}

	int16_t tmp_pid;
	list_for_each_entry(tsym, t_it, symbol_listp) {
		c_it = &tsym->col_sym_list;
		list_for_each_entry(ccs, c_it, col_sym_listp) {
			for (i = 0; i < n_cluster; i++) {
				if ((c_type[i] == OP_AND) && ((cluster_t *) ccs->ptr == cluster[i]))
					break;
			}

			// from independent cluster
			if (i >= n_cluster)
				continue;

			tmp_pid = least_pid[i];
			qp = (qapla_policy_t *) get_pol_for_pid(qm, tmp_pid);
			if (!qp_list[tmp_pid])
				n_distinct_pol++;

			qp_list[tmp_pid] = qp;
			ccs->ptr = (void *) qp;
		}
	}

	if (c_type)
		free(c_type);
	if (c_used)
		free(c_used);
	if (cluster)
		free(cluster);
	if (c_pvec)
		free(c_pvec);
	if (least_pid)
		free(least_pid);

	*n_qp = n_distinct_pol;
	return POL_EVAL_SUCCESS;
}

static int
mysql_get_sql_for_col(char *out, char *cname, char *pol, int pol_len)
{
	char *col_rewrite_ptr = out;
	memcpy(col_rewrite_ptr, "(if (", 5);
	col_rewrite_ptr += 5;
	memcpy(col_rewrite_ptr, pol, pol_len-1);
	col_rewrite_ptr += pol_len-1;
	memcpy(col_rewrite_ptr, ", ", 2);
	col_rewrite_ptr += 2;
	memcpy(col_rewrite_ptr, cname, strlen(cname));
	col_rewrite_ptr += strlen(cname);
	memcpy(col_rewrite_ptr, ", NULL)) ", 9);
	col_rewrite_ptr += 9;
	memcpy(col_rewrite_ptr, cname, strlen(cname));
	col_rewrite_ptr += strlen(cname);

  return 0;
}

static int
general_get_sql_for_col(char *out, char *cname, char *pol, int pol_len)
{
	char *col_rewrite_ptr = out;
	char *case_str = (char *) "(case when (";
	char *then_str = (char *) ") then ";
	char *end_str = (char *) " else NULL end) as ";
	memcpy(col_rewrite_ptr, case_str, strlen(case_str));
	col_rewrite_ptr += strlen(case_str);
	memcpy(col_rewrite_ptr, pol, pol_len-1);
	col_rewrite_ptr += pol_len-1;
	memcpy(col_rewrite_ptr, then_str, strlen(then_str));
	col_rewrite_ptr += strlen(then_str);
	memcpy(col_rewrite_ptr, cname, strlen(cname));
	col_rewrite_ptr += strlen(cname);
	memcpy(col_rewrite_ptr, end_str, strlen(end_str));
	col_rewrite_ptr += strlen(end_str);
	memcpy(col_rewrite_ptr, cname, strlen(cname));
	col_rewrite_ptr += strlen(cname);

  return 0;
}

static int
get_sql_for_table(char **curr_q, int *curr_qlen, char *tname, list_t *cs_list)
{
	if (!curr_q || !curr_qlen || !cs_list)
		return -1;

	char *select_str = (char *) "select ";
	char *from_str = (char *) "from ";
	char *comma_str = (char *) ",\n";
	int select_str_len = strlen(select_str);
	int from_str_len = strlen(from_str);
	int comma_str_len = strlen(comma_str);
	int tname_str_len = strlen(tname);

	int n_col = 0, it = 0;
	int tab_sql_len = 0;
	list_t *c_it = cs_list;
	col_sym_t *cs;
	char *cname;

	list_for_each_entry(cs, c_it, col_sym_listp) {
		//tab_sql_len += strlen((char *) cs->ptr);
		tab_sql_len += cs->ptr_size;
		n_col++;
	}
	tab_sql_len += select_str_len + from_str_len + tname_str_len + 
		((n_col-1) * comma_str_len) + 1; // +1 for space after the last project col expr

	char *sql = (char *) malloc(tab_sql_len+1);
	memset(sql, 0, tab_sql_len+1);
	char *ptr = sql;

	memcpy(ptr, select_str, select_str_len);
	ptr += select_str_len;
	c_it = cs_list;
	list_for_each_entry(cs, c_it, col_sym_listp) {
		//memcpy(ptr, (char *) cs->ptr, strlen((char *) cs->ptr));
		//ptr += strlen((char *) cs->ptr);
		memcpy(ptr, (char *) cs->ptr, cs->ptr_size);
		ptr += cs->ptr_size;
		it++;
		if (it == n_col)
			break;
		memcpy(ptr, comma_str, comma_str_len);
		ptr += comma_str_len;
	}

	memcpy(ptr, " ", 1);
	ptr += 1;
	memcpy(ptr, from_str, from_str_len);
	ptr += from_str_len;
	memcpy(ptr, tname, tname_str_len);
	ptr += tname_str_len;

	*curr_q = sql;
	*curr_qlen = tab_sql_len+1;

	return 0;
}

int
mysql_query_get_sql_for_table(query_info_int_t *qi, list_t *table_list,
		char **col_sql, int *col_sql_len, char ***tab_sql_str,
		int **tab_sql_str_len, int *n_tab_sql)
{
	if (!col_sql || !col_sql_len)
		return POL_EVAL_ERROR;

	list_t *t_it, *c_it;
	symbol_t *tsym;
	col_sym_t *ccs;

	char *tname, *cname;

	char *col_rewrite_buf, *col_rewrite_ptr;
	int col_rewrite_buf_len = 0;
	int16_t pid;
	
	// "(if (<pol>, <cname>, NULL)) <cname>"
	const char *rewrite_template = "(if (, , NULL)) ";
	int rewrite_template_len = strlen(rewrite_template);

	qapla_policy_t *tmp_qp;

	int n_tab = 0;
	t_it = table_list;
	list_for_each_entry(tsym, t_it, symbol_listp)
		n_tab++;

	char **tab_sql = (char **) malloc(sizeof(char *) * n_tab);
	int *tab_sql_len = (int *) malloc(sizeof(int) * n_tab);
	memset(tab_sql, 0, sizeof(char *) * n_tab);
	memset(tab_sql_len, 0, sizeof(int) * n_tab);
	int tab_idx = 0;

	t_it = table_list;
	list_for_each_entry(tsym, t_it, symbol_listp) {
		tname = sym_field(tsym, db_tname);

		c_it = &tsym->col_sym_list;
		list_for_each_entry(ccs, c_it, col_sym_listp) {
			tmp_qp = (qapla_policy_t *) ccs->ptr;
			pid = qapla_get_policy_id(tmp_qp);

			cname = sym_field(ccs, db_cname);
			col_rewrite_buf_len = rewrite_template_len + (col_sql_len[pid]-1) +
				(2*strlen(cname));
			col_rewrite_buf = (char *) malloc(col_rewrite_buf_len+1);
			memset(col_rewrite_buf, 0, col_rewrite_buf_len+1);
			mysql_get_sql_for_col(col_rewrite_buf, cname, col_sql[pid], col_sql_len[pid]);
			ccs->ptr = (void *) col_rewrite_buf;
			ccs->ptr_size = col_rewrite_buf_len;
		}

		get_sql_for_table(&tab_sql[tab_idx], &tab_sql_len[tab_idx], tname, 
				&tsym->col_sym_list);
		tab_idx++;
	}

	*tab_sql_str = tab_sql;
	*tab_sql_str_len = tab_sql_len;
	*n_tab_sql = n_tab;

	// == cleanup ==
	t_it = table_list;
	list_for_each_entry(tsym, t_it, symbol_listp) {
		c_it = &tsym->col_sym_list;
		list_for_each_entry(ccs, c_it, col_sym_listp) {
			if (ccs->ptr) {
				memset(ccs->ptr, 0, ccs->ptr_size);
				free(ccs->ptr);
				ccs->ptr_size = 0;
			}
		}
	}

	return POL_EVAL_SUCCESS;
}

inline int
general_query_get_sql_for_table(query_info_int_t *qi, list_t *table_list,
		char **col_sql, int *col_sql_len, char ***tab_sql_str,
		int **tab_sql_str_len, int *n_tab_sql)
{
	if (!col_sql || !col_sql_len)
		return POL_EVAL_ERROR;

	list_t *t_it, *c_it;
	symbol_t *tsym;
	col_sym_t *ccs;

	char *tname, *cname;

	char *col_rewrite_buf, *col_rewrite_ptr;
	int col_rewrite_buf_len = 0;
	int16_t pid;
	
	// "(case when (<pol>) then <cname> else NULL end) as <cname>"
	const char *rewrite_template = "(case when () then  else NULL end) as ";
	int rewrite_template_len = strlen(rewrite_template);

	qapla_policy_t *tmp_qp;

	int n_tab = 0;
	t_it = table_list;
	list_for_each_entry(tsym, t_it, symbol_listp)
		n_tab++;

	char **tab_sql = (char **) malloc(sizeof(char *) * n_tab);
	int *tab_sql_len = (int *) malloc(sizeof(int) * n_tab);
	memset(tab_sql, 0, sizeof(char *) * n_tab);
	memset(tab_sql_len, 0, sizeof(int) * n_tab);
	int tab_idx = 0;

	t_it = table_list;
	list_for_each_entry(tsym, t_it, symbol_listp) {
		tname = sym_field(tsym, db_tname);

		c_it = &tsym->col_sym_list;
		list_for_each_entry(ccs, c_it, col_sym_listp) {
			tmp_qp = (qapla_policy_t *) ccs->ptr;
			pid = qapla_get_policy_id(tmp_qp);

			cname = sym_field(ccs, db_cname);
			col_rewrite_buf_len = rewrite_template_len + (col_sql_len[pid]-1) +
				(2*strlen(cname));
			col_rewrite_buf = (char *) malloc(col_rewrite_buf_len+1);
			memset(col_rewrite_buf, 0, col_rewrite_buf_len+1);
			general_get_sql_for_col(col_rewrite_buf, cname, col_sql[pid], col_sql_len[pid]);
			ccs->ptr = (void *) col_rewrite_buf;
			ccs->ptr_size = col_rewrite_buf_len;
		}

		get_sql_for_table(&tab_sql[tab_idx], &tab_sql_len[tab_idx], tname, 
				&tsym->col_sym_list);
		tab_idx++;
	}

	*tab_sql_str = tab_sql;
	*tab_sql_str_len = tab_sql_len;
	*n_tab_sql = n_tab;

	// == cleanup ==
	t_it = table_list;
	list_for_each_entry(tsym, t_it, symbol_listp) {
		c_it = &tsym->col_sym_list;
		list_for_each_entry(ccs, c_it, col_sym_listp) {
			if (ccs->ptr) {
				memset(ccs->ptr, 0, ccs->ptr_size);
				free(ccs->ptr);
				ccs->ptr_size = 0;
			}
		}
	}

	return POL_EVAL_SUCCESS;
}

int
query_get_sql_for_table(query_info_int_t *qi, list_t *table_list, char **col_sql,
		int *col_sql_len, char ***tab_sql_str, int **tab_sql_str_len, int *n_tab_sql)
{
	// case-when rewrite can be used in all databases
	return general_query_get_sql_for_table(qi, table_list, col_sql, col_sql_len, 
			tab_sql_str, tab_sql_str_len, n_tab_sql);
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
