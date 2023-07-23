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
#include "pol_eval.h"

#include <antlr3.h>
#include <glib.h>

void
error_print_col_link(FILE *f, col_sym_t *cs)
{
	fprintf(f, "conflicting policies in link cluster, "
			"all bits set to 0 due to col: %s:%s.%s(%d.%d)\n",
			sym_field(cs, name), sym_field(cs, db_tname), 
			sym_field(cs, db_cname), sym_field(cs, db_tid), 
			sym_field(cs, db_cid));
}

int
is_aggr_query(query_info_int_t *qi)
{
	context_t *firstC = get_first_context(&qi->pc);
	if (!firstC)
		return -1;

	return firstC->is_aggr_query;
}

int 
aggr_query_get_pvec(query_info_int_t *qi, PVEC *pv)
{
	context_t *firstC = get_first_context(&qi->pc);
	if (!firstC || !firstC->is_aggr_query)
		return -1;

	metadata_t *qm = &qi->qm;
	cluster_t *aggr_cluster;
	list_t *cluster_list = metadata_get_cluster_list(qm);
	int n_cluster = metadata_get_num_clusters(qm);
	int cnt = 0;
	list_for_each_entry(aggr_cluster, cluster_list, cluster_listp) {
		cnt++;
		if (cnt == n_cluster)
			break;
	}

	PVEC c_g_pvec = -1, g_tmp_pv = 0, log_tmp_pv = 0;
	symbol_t *sym;
	col_sym_t *cs;
	list_t *cs_list, *col_list;
	int ret = 0, fail = 0;
	
	col_list = &firstC->symbol_list[SYM_COL];

	list_for_each_entry(sym, col_list, symbol_listp) {
		cs_list = &sym->col_sym_list;
		list_for_each_entry(cs, cs_list, col_sym_listp) {
			g_tmp_pv = 0;
			ret = cluster_get_pid_for_col(aggr_cluster, cs, &g_tmp_pv);
			if (ret < 0) {
				fail = 1;
				break;
			}

			c_g_pvec &= g_tmp_pv;
#if DEBUG
			printf("cs:(%s,%s) tmp: %lu, new pv: %lu\n", sym_field(cs, db_tname),
					sym_field(cs, db_cname), g_tmp_pv, c_g_pvec);
#endif
		}
		if (fail == 1)
			break;
	}

	// query does not satisfy aggregate policy
	if (fail == 1)
		return -1;

#if DEBUG
	printf("aggr pvec: 0x%lx\n", c_g_pvec);
#endif

	// conflicting policies detected
	if (c_g_pvec == 0)
		return -1;

	int16_t least_pid = -1;
	PVEC tmp_pv = 0;
	least_pid = get_least_restrictive_pid(aggr_cluster, c_g_pvec);
#if DEBUG
		printf("aggr least pid: %d\n", least_pid);
#endif
	if (least_pid < 0)
		return -1;

	tmp_pv |= ((PVEC) 1 << least_pid);
	c_g_pvec = tmp_pv;

#if 0
	// more than one aggregate policy applicable to query
	int16_t pid = get_pid_from_pvec(c_g_pvec);
	if (pid < 0)
		return -1;
#endif

	*pv = c_g_pvec;
	return 0;
}

int
get_col_list_for_eval(list_t *actual_list, list_t *expected_list, 
		list_t *unexpected_list)
{
	symbol_t *a_sym;
	col_sym_t *a_cs, *match_cs;
	list_t *a_cs_list;
	list_t *mark_list = NULL;
	int expected = -1;

	if (!list_empty(unexpected_list)) {
		mark_list = unexpected_list;
		expected = 0;
	} else if (!list_empty(expected_list)) {
		mark_list = expected_list;
		expected = 1;
	}
	
	if (mark_list) {
		list_for_each_entry(a_sym, actual_list, symbol_listp) {
			a_cs_list = &a_sym->col_sym_list;
			list_for_each_entry(a_cs, a_cs_list, col_sym_listp) {
				if (exists_col_sym_in_list(a_cs, mark_list, 0, &match_cs) == 0)
					a_cs->is_expected = expected;
				else
					a_cs->is_expected = !expected;
				a_cs->is_set = 1;
			}
		}
	} else {
		list_for_each_entry(a_sym, actual_list, symbol_listp) {
			a_cs_list = &a_sym->col_sym_list;
			list_for_each_entry(a_cs, a_cs_list, col_sym_listp) {
				a_cs->is_expected = 1;
				a_cs->is_set = 1;
			}
		}
	}

	return 0;
}

int
query_get_pvec(query_info_int_t *qi, PVEC *pv)
{
	context_t *currC = get_curr_context(&qi->pc);
	if (!currC)
		return -1;

	list_t *col_list = &currC->symbol_list[SYM_COL];
	list_t *expected_list = qstr_get_expected_col_list(&qi->qstr);
	list_t *unexpected_list = qstr_get_unexpected_col_list(&qi->qstr);
	get_col_list_for_eval(col_list, expected_list, unexpected_list);

	metadata_t *qm = &qi->qm;
    // XXX LYT why is there a -1?
    // got rid of it and it now works
	int n_cluster = metadata_get_num_clusters(qm);// - 1;
	cluster_t *false_cluster = metadata_get_false_cluster(qm);
	PVEC *c_g_pvec = (PVEC *) malloc(sizeof(PVEC) * n_cluster);
	uint8_t *c_used = (uint8_t *) malloc(sizeof(uint8_t) * n_cluster);
	uint8_t *c_type = (uint8_t *) malloc(sizeof(uint8_t) * n_cluster);
	cluster_t **cluster = (cluster_t **) malloc(sizeof(cluster_t *) * n_cluster);
	memset(c_g_pvec, 0, sizeof(PVEC) * n_cluster);
	memset(c_used, 0, sizeof(uint8_t) * n_cluster);
	memset(c_type, 0, sizeof(uint8_t) * n_cluster);
	memset(cluster, 0, sizeof(cluster_t *) * n_cluster);

#if DEBUG
        printf("n_cluster: %d\n", n_cluster);
#endif

	int i, seen_one = 0;
	symbol_t *sym;
	col_sym_t *cs;
	list_t *cs_list;
	PVEC g_tmp_pv = 0, log_tmp_pv = 0, final_pv = -1;
	cluster_t *tmp_c;
	int cluster_id = 0, cluster_op = -1;
	int ret, fail = 0;
	list_for_each_entry(sym, col_list, symbol_listp) {
#if DEBUG
        printf("Looking up entry in sym list\n");
#endif

		cs_list = &sym->col_sym_list;
		seen_one = 0;
		list_for_each_entry(cs, cs_list, col_sym_listp) {
#if DEBUG
            printf("Looking up entry in col list\n");
#endif
			g_tmp_pv = 0;
			// don't include pvec of columns not expected in the result
			if (cs->is_expected == 0) {
#if DEBUG
                printf("col not expected\n");
#endif
				continue;
            }
			// ignore symbols from constant queries
			int tid = sym_field(cs, db_tid);
			if (tid == DUAL_TID) {
#if DEBUG
                printf("dual tid\n");
#endif
            	continue;
            }

			ret = metadata_lookup_cluster_pvec(qm, cs, &tmp_c, &g_tmp_pv);
			if (ret < 0) { 
#if DEBUG
				int orig_qlen = 0;
				char *orig_q = qstr_get_orig_query(&qi->qstr, &orig_qlen);
				fprintf(stderr, "[%lu] failed policy lookup for col: %s:%s.%s(%d.%d), ..... Q:%s\n",
						getpid(), sym_field(cs, name), sym_field(cs, db_tname), 
						sym_field(cs, db_cname), sym_field(cs, db_tid), sym_field(cs, db_cid), 
						orig_q);
#endif
#if 0
				// no explicit policy defined; apply default false policy on col's table
				fail = 1;
				cluster_id = cluster_get_id(false_cluster);
				cluster_op = cluster_get_op_type(false_cluster);
				c_type[cluster_id] = cluster_op;
				if (!c_used[cluster_id]) {
					cluster[cluster_id] = false_cluster;
					c_g_pvec[cluster_id] = 0;
					c_used[cluster_id] = 1;
				}
				
				list_t tab_cs_list;
				list_init(&tab_cs_list);
				col_sym_t *tab_cs, *tmp_cs;
				tmp_cs = list_entry(cs_list->next, col_sym_t, col_sym_listp);
				tab_cs = (col_sym_t *) malloc(sizeof(col_sym_t));
				init_col_sym(tab_cs);
				int tid = sym_field(tmp_cs, db_tid);
				set_col_sym_tid_cid(tab_cs, tid, tid);
				list_insert(&tab_cs_list, &tab_cs->col_sym_listp);
				g_tmp_pv = 0;
				ret = cluster_get_pid_for_col_list(false_cluster, &tab_cs_list, &g_tmp_pv);

				c_g_pvec[cluster_id] |= g_tmp_pv;
				free_col_sym(&tab_cs);
#endif
			} else {
				cluster_id = cluster_get_id(tmp_c);
				cluster_op = cluster_get_op_type(tmp_c);
				c_type[cluster_id] = cluster_op;
				if (!c_used[cluster_id]) {
					cluster[cluster_id] = tmp_c;
					if (cluster_op == OP_AND)
						c_g_pvec[cluster_id] = -1;
					else
						c_g_pvec[cluster_id] = 0;
					c_used[cluster_id] = 1;
				}
				if (cluster_op == OP_AND) {
					c_g_pvec[cluster_id] &= g_tmp_pv;
					if (c_g_pvec[cluster_id] == 0) {
						error_print_col_link(qi->log_info.f, cs);
					}
				} else {
					c_g_pvec[cluster_id] |= g_tmp_pv;
				}
			}
#if DEBUG
			printf("cs:(%s,%s) cluster: %d, op: %d, tmp: %lu, new pv: %lu\n",
					sym_field(cs, db_tname), sym_field(cs, db_cname), 
					cluster_id, cluster_op, g_tmp_pv, c_g_pvec[cluster_id]);
#endif
		}
	}

#if 0
	if (fail) {
		if (c_type)
			free(c_type);
		if (c_used)
			free(c_used);
		if (cluster)
			free(cluster);
		if (c_g_pvec)
			free(c_g_pvec);
		return -1;
	}
#endif

	// if more than one policies set in a lattice, pick the least restrictive one
	int16_t least_pid = -1;
	PVEC tmp_pvec = 0;
	for (i = 0; i < n_cluster; i++) {
		if (!cluster[i]) {
#if DEBUG
            printf("Not a cluster %d\n", i);
#endif
			continue;
        }

		if (c_type[i] == OP_OR){
#if DEBUG
            printf("op_or %d\n", i);
#endif
			continue;
        }

		if (c_g_pvec[i] == 0) {
			fprintf(qi->log_info.f, "Qapla ERROR!! link policy is false, cluster: %d\n", i);
#if DEBUG
            printf("link policy is false, cluster %d\n", i);
#endif
		}
		least_pid = get_least_restrictive_pid(cluster[i], c_g_pvec[i]);
		if (least_pid < 0) {
#if DEBUG
            printf("least pid %d\n", i);
#endif
			continue;
        }

		tmp_pvec |= ((PVEC) 1 << least_pid);
#if DEBUG
		printf("%d. least pid: %d\n", i, least_pid);
#endif
		c_g_pvec[i] = tmp_pvec;
	}

	// policies from different clusters are independent, 
	// all of them must be applied in conjunction, set all bits
	final_pv = 0;
	int any_policy = 0;
	for (i = 0; i < n_cluster; i++) {
		if (!c_used[i])
			continue;

		final_pv |= c_g_pvec[i];
		any_policy = 1;
	}

	if (c_type)
		free(c_type);
	if (c_used)
		free(c_used);
	if (cluster)
		free(cluster);
	if (c_g_pvec)
		free(c_g_pvec);

	if (any_policy) {
		*pv = final_pv;
		return 0;
	}

	return -1;
}

int
get_sql_clauses_in_pol(qapla_policy_t *qp, query_info_int_t *qi, uint64_t tid, char **q,
		int *qlen)
{
	int i, dlog_ret, sql_ret = 0;
	// == init dlog interpreter env ==
	dlog_pi_env_t env;
	dlog_ret = dlog_pi_env_init(&env, qp, QP_PERM_READ, &qi->session);

	int num_clause = qapla_get_num_perm_clauses(qp, QP_PERM_READ);
	char **sql_resolved = (char **) malloc(sizeof(char *) * num_clause);
	int *sql_resolved_len = (int *) malloc(sizeof(int) * num_clause);
	memset(sql_resolved, 0, sizeof(char *) * num_clause);
	memset(sql_resolved_len, 0, sizeof(int) * num_clause);
	int n_valid_clause = 0;

	char *newquery;
	int newquery_len = 0;
	char *or_str = (char *) "or";
	int or_str_len = strlen("or") + 1;
	char *false_str = (char *) "1=0";
	char *true_str = (char *) "1=1";
	int false_str_len = strlen(false_str);
	int true_str_len = strlen(true_str);

	qapla_policy_t out_qp_buf, *out_qp;
	out_qp = &out_qp_buf;
	// split sql_parse_params into per-tid?
	list_t sym_list;
	list_init(&sym_list);
	symbol_t *sym = (symbol_t *) malloc(sizeof(symbol_t));
	init_symbol(sym);
	set_sym_field(sym, db_tid, tid);
	list_insert(&sym_list, &sym->symbol_listp);

	for (i = 0; i < num_clause; i++) {
		dlog_ret = dlog_pi_evaluate(qp, QP_PERM_READ, i, &env);
		if (dlog_ret != DLOG_PI_GRANTED_PERM)
			continue;

#if (CONFIG_REFMON == RM_CACHE_FULL_POLICY)
		memset(&out_qp_buf, 0, sizeof(qapla_policy_t));
		sql_ret = sql_parse_params(qp, QP_PERM_READ, i, &env, out_qp, &sym_list);

		if (sql_ret == SQL_PI_FAILURE) {
			dlog_pi_env_unsetAllVars(&env);
			continue;
		}

		if (sql_ret == SQL_PI_DISALLOWED) {
			sql_resolved[n_valid_clause] = strndup(false_str, false_str_len);
			sql_resolved_len[n_valid_clause] = false_str_len + 1;
		} else if (sql_ret == SQL_PI_GRANTED_PERM) {
			sql_resolved[n_valid_clause] = strndup(true_str, true_str_len);
			sql_resolved_len[n_valid_clause] = true_str_len + 1;
		} else {
			char *cls = qapla_get_perm_clause_start(out_qp, QP_PERM_READ, CTYPE_SQL, 0);
#else
			char *cls = qapla_get_perm_clause_start(qp, QP_PERM_READ, CTYPE_SQL, i);
#endif
			dlog_pi_op_t *sql_op = (dlog_pi_op_t *) cls;
			char *sql = get_var_length_ptr(&sql_op->tuple, 1);
			uint16_t sql_len = get_var_length_ptr_len(&sql_op->tuple, 1);
			sql_resolved[n_valid_clause] = (char *) malloc(sql_len);
			memset(sql_resolved[n_valid_clause], 0, sql_len);
			memcpy(sql_resolved[n_valid_clause], sql, sql_len - 1);
			sql_resolved_len[n_valid_clause] = sql_len;
#if (CONFIG_REFMON == RM_CACHE_FULL_POLICY)
		}
#endif
		newquery_len += sql_resolved_len[n_valid_clause];
		n_valid_clause++;
		// unset env before evaluating next clause
		dlog_pi_env_unsetAllVars(&env);
	}

	if (n_valid_clause > 0)
		newquery_len += ((n_valid_clause - 1) * or_str_len);
	else
		newquery_len += false_str_len + 1;
	newquery_len += 2; // for adding parantheses

	newquery = (char *) malloc(newquery_len);
	memset(newquery, 0, newquery_len);
	char *ptr = newquery;
	memcpy(ptr, "(", 1);
	ptr += 1;
	if (n_valid_clause > 0) {
		for(i = 0; i < n_valid_clause - 1; i++) {
			memcpy(ptr, sql_resolved[i], sql_resolved_len[i]-1);
			ptr += (sql_resolved_len[i] - 1);
			memcpy(ptr, " ", 1);
			ptr += 1;
			memcpy(ptr, or_str, or_str_len - 1);
			ptr += (or_str_len - 1);
			memcpy(ptr, " ", 1);
			ptr += 1;
		}
		memcpy(ptr, sql_resolved[i], sql_resolved_len[i] - 1);
		ptr += (sql_resolved_len[i] - 1);
	} else {
		memcpy(ptr, false_str, false_str_len);
		ptr += false_str_len;
	}
	memcpy(ptr, ")", 1);
	ptr += 1;

#if DEBUG
	printf("computed new query len: %d, ptr-newquery: %d\n", newquery_len,
			(int) (ptr - newquery));
#endif

	*q = newquery;
	*qlen = newquery_len;

	dlog_pi_env_cleanup(&env);

	list_remove(&sym->symbol_listp);
	list_init(&sym_list);
	free(sym);
	sym = NULL;

	for (i = 0; i < n_valid_clause; i++)
		free(sql_resolved[i]);
	free(sql_resolved);
	free(sql_resolved_len);

	return 0;
}

int
add_sql_to_table(char **curr_q, int *curr_qlen, char *q, int qlen)
{
	if (!curr_q || !q || !curr_qlen || !qlen)
		return -1;

	char *and_str = (char *) "AND";
	int and_str_len = strlen("AND");
	int new_qlen = qlen;
	new_qlen = new_qlen + (*curr_qlen > 0 ? (*curr_qlen+and_str_len+1) : 0);
	char *new_q = (char *) malloc(new_qlen);
	memset(new_q, 0, new_qlen);
	char *ptr = new_q;
	if (*curr_qlen) {
		memcpy(ptr, *curr_q, *curr_qlen - 1);
		ptr += *curr_qlen - 1;
		memcpy(ptr, " ", 1);
		ptr += 1;
		memcpy(ptr, and_str, and_str_len);
		ptr += and_str_len;
		memcpy(ptr, " ", 1);
		ptr += 1;
	}
	memcpy(ptr, q, qlen - 1);
	ptr += qlen - 1;

	if (*curr_qlen) {
		free(*curr_q);
	}
	*curr_q = new_q;
	*curr_qlen = new_qlen;

	return 0;
}

int
query_pvec_get_policy_list(query_info_int_t *qi, PVEC pv, qapla_policy_t ***pol_list, int *n_pol_list)
{
	int i, pid_it, is_set, n_pid = 0;
	PVEC i_pvec = 0;
	uint16_t *pid;
	qapla_policy_t **qp;
	for (i = 0; i < MAX_POLICIES; i++) {
		i_pvec = (PVEC) (pv >> i);
		is_set = (int) (i_pvec & 1);
		if (is_set == 0)
			continue;

#if DEBUG
		printf("query apply policy, pid: %d\n", i);
#endif
		n_pid++;
	}

	pid = (uint16_t *) malloc(sizeof(uint16_t) * n_pid);
	qp = (qapla_policy_t **) malloc(sizeof(qapla_policy_t *) * n_pid);
	pid_it = 0;
	for (i = 0; i < MAX_POLICIES; i++) {
		i_pvec = (PVEC) (pv >> i);
		is_set = (int) (i_pvec & 1);
		if (is_set == 0)
			continue;

		pid[pid_it] = (uint16_t) i;
		qp[pid_it] = (qapla_policy_t *) get_pol_for_pid(&qi->qm, pid[pid_it]);
		pid_it++;
	}

	*pol_list = qp;
	*n_pol_list = n_pid;
	free(pid);

	return 0;
}

int
free_policy_list(qapla_policy_t ***pol_list, int n_pol_list)
{
	if (!pol_list || !(*pol_list))
		return 0;

	qapla_policy_t **qp = *pol_list;
	free(qp);
	*pol_list = NULL;
	return 0;
}

int
dedupe_col_list(list_t *dedupe_list, col_sym_t *cs)
{
	col_sym_t *dedupe_cs, *match_cs;
	if (exists_col_sym_in_list(cs, dedupe_list, 0, &match_cs) == 0)
		return 1;

	dedupe_cs = dup_col_sym(cs);
	list_insert(dedupe_list, &dedupe_cs->col_sym_listp);
	return 0;
}

static int
get_sql_for_table(char **curr_q, int *curr_qlen, char *tname, db_t *schema,
		char **p_arr, int *p_len_arr, int n_pid, 
		list_t *expected_col_list, list_t *unexpected_col_list, list_t *actual_list)
{
	if (!curr_q)
		return -1;

	char *select_str = (char *) "select ";
	char *all_str = (char *) "* ";
	char *from_str = (char *) "from ";
	char *where_str = (char *) "where ";
	char *and_str = (char *) "AND ";
	int select_str_len = strlen(select_str);
	int all_str_len = strlen(all_str);
	int from_str_len = strlen(from_str);
	int where_str_len = strlen(where_str);
	int and_str_len = strlen(and_str);
	int tname_str_len = strlen(tname);
	int all_expected = 0;

	int i, j = 0;
	int n_valid_pol = 0;
	int new_qlen = 0;
	char *new_q = NULL, *ptr;
	int tid = get_schema_table_id(schema, tname);
	int n_col = get_schema_num_col_in_table_id(schema, tid);

	int select_col_str_len = n_col * MAX_NAME_LEN;
	int curr_select_col_str_len = 0;
	int first_col = 1;
	char *select_col_str = (char *) malloc(select_col_str_len);
	memset(select_col_str, 0, select_col_str_len);

	list_t dedupe_cs_list;
	list_init(&dedupe_cs_list);

	if ((!expected_col_list || list_empty(expected_col_list)) &&
			(!unexpected_col_list || list_empty(unexpected_col_list)))
		all_expected = 1;

	for (i = 0; i < n_pid; i++) {
		if (p_arr[i] && p_len_arr[i]) {
			n_valid_pol++;
			new_qlen += (p_len_arr[i] - 1);
			new_qlen += 1; // +1 for space after each policy string
		}
	}
	new_qlen += (n_valid_pol - 1) * and_str_len;
	new_qlen += select_str_len + from_str_len + where_str_len + tname_str_len + 1;
	if (all_expected)
		new_qlen += all_str_len;
	else {
		// compute new_qlen taking into account the expected and unexpected list
		symbol_t *a_sym;
		col_sym_t *a_cs;
		list_t *a_cs_list;
		list_for_each_entry(a_sym, actual_list, symbol_listp) {
			a_cs_list = &a_sym->col_sym_list;
			list_for_each_entry(a_cs, a_cs_list, col_sym_listp) {
				if (sym_field(a_cs, db_tid) != tid)
					continue;

				if (dedupe_col_list(&dedupe_cs_list, a_cs))
					continue;

				if (!first_col) {
					sprintf(select_col_str + curr_select_col_str_len, ", ");
					curr_select_col_str_len = strlen(select_col_str);
				}
				if (a_cs->is_expected) {
					sprintf(select_col_str + curr_select_col_str_len, "%s.%s", 
							tname, sym_field(a_cs, db_cname));
					curr_select_col_str_len = strlen(select_col_str);
					if (first_col)
						first_col = 0;
				} else if (a_cs->is_set && a_cs->is_expected == 0) {
					sprintf(select_col_str + curr_select_col_str_len, "null %s", 
							sym_field(a_cs, db_cname));
					curr_select_col_str_len = strlen(select_col_str);
					if (first_col)
						first_col = 0;
				} else {
					assert(0);
				}
			}
		}
		curr_select_col_str_len = strlen(select_col_str);
		new_qlen += strlen(select_col_str) + 1; // +1 for adding a space after the list
	}

	new_q = (char *) malloc(new_qlen);
	memset(new_q, 0, new_qlen);
	ptr = new_q;

	memcpy(ptr, select_str, select_str_len);
	ptr += select_str_len;
	if (all_expected) {
		memcpy(ptr, all_str, all_str_len);
		ptr += all_str_len;
	} else {
		memcpy(ptr, select_col_str, curr_select_col_str_len);
		ptr += curr_select_col_str_len;
		memcpy(ptr, " ", 1);
		ptr += 1;
	}
	memcpy(ptr, from_str, from_str_len);
	ptr += from_str_len;
	memcpy(ptr, tname, tname_str_len);
	ptr += tname_str_len;
	memcpy(ptr, " ", 1);
	ptr += 1;
	memcpy(ptr, where_str, where_str_len);
	ptr += where_str_len;

	j = 0;
	for (i = 0; i < n_pid; i++) {
		if (p_arr[i] && p_len_arr[i]) {
			j++;
			memcpy(ptr, p_arr[i], p_len_arr[i] - 1);
			ptr += p_len_arr[i] - 1;

			if (j == n_valid_pol)
				break;
			
			memcpy(ptr, " ", 1);
			ptr += 1;
			memcpy(ptr, and_str, and_str_len);
			ptr += and_str_len;
		}
	}

	*curr_q = new_q;
	*curr_qlen = new_qlen;

	// cleanup allocated memory
	if (select_col_str)
		free(select_col_str);
	cleanup_col_sym_list(&dedupe_cs_list);

	return 0;
}

int
get_resolved_sql_pol(query_info_int_t *qi, qapla_policy_t **qp, int n_pid, 
		qapla_perm_id_t perm, char ***tab_sql_str, int **tab_sql_str_len, 
		int *n_tab_sql, list_t *table_list)
{
	if (!qp || !(*qp))
		return -1;

	context_t *currC = get_curr_context(&qi->pc);
	list_t *tab_list = &currC->symbol_list[SYM_TAB];
	list_t *actual_list = &currC->symbol_list[SYM_COL];
	qstr_t *qstr = &qi->qstr;
	list_t *expected_list = qstr_get_expected_col_list(qstr);
	list_t *unexpected_list = qstr_get_unexpected_col_list(qstr);

	int is_match = 0;
	int ret, i;

	// count # tables in query, for which policies must be applied
	uint64_t tid;
	char *tname;
	symbol_t *new_tsym;
	int n_tab = 0;
	uint64_t tab_arr[10]; // approx., there may not be more than 10 tables in a single query
	int max_tab = 10;
	memset(tab_arr, 0, sizeof(uint64_t) * max_tab);
	ret = get_distinct_table_array(tab_arr, &n_tab, max_tab, tab_list);
	assert(!ret);

	for (i = 0; i < n_tab; i++) {
		tid = tab_arr[i];
		assert(((int)tid >= 0) && ((int) tid != DUAL_TID));
		//if ((int) tid < 0)
		//	continue;

		tname = get_schema_table_name(&qi->schema, (int) tid);
		new_tsym = (symbol_t *) malloc(sizeof(symbol_t));
		init_symbol(new_tsym);
		set_sym_str_field(new_tsym, db_tname, tname);
		set_sym_field(new_tsym, db_tid, tid);
		list_insert_at_tail(table_list, &new_tsym->symbol_listp);
	}

	// array of final policy string for each table
	char **tab_sql = (char **) malloc(sizeof(char *) * n_tab);
	int *tab_sql_len = (int *) malloc(sizeof(int) * n_tab);
	memset(tab_sql, 0, sizeof(char *) * n_tab);
	memset(tab_sql_len, 0, sizeof(int) * n_tab);
	int tab_idx = 0;

	// array to hold policy strings to be appended to a table
	char **pol_for_tab = (char **) malloc(sizeof(char *) * n_pid);
	int *pol_for_tab_len = (int *) malloc(sizeof(int) * n_pid);
	memset(pol_for_tab, 0, sizeof(char *) * n_pid);
	memset(pol_for_tab_len, 0, sizeof(int) * n_pid);

	/*
	 * for each tid
	 * 	for each policy qp in pvec list
	 * 		if policy applied to cs.db_tid
	 * 			for each clause in qp
	 * 				if eval(qp.dlog) = success
	 * 					add qp.sql to pol_buf with an OR
	 * 		append pol_buf to tab_pol_buf[db_tid] with an AND
	 */
	int tab_it;
	int q_pid;
	for (tab_it = 0; tab_it < n_tab; tab_it++) {
		tid = tab_arr[tab_it];
		tname = get_schema_table_name(&qi->schema, tid);
		tab_idx = get_table_index(tab_arr, n_tab, tid);
		memset(pol_for_tab, 0, sizeof(char *) * n_pid);

		for (i = 0; i < n_pid; i++) {
			is_match = match_sql_table_in_pol(qp[i], perm, tid);
#if DEBUG
			q_pid = qapla_get_policy_id(qp[i]);
			printf("query tid: %d pol id: %d, match: %d\n", tid, q_pid, is_match);
#endif
			if (!is_match)
				continue;

			ret = get_sql_clauses_in_pol(qp[i], qi, tid, &pol_for_tab[i], 
					&pol_for_tab_len[i]);
		}
		get_sql_for_table(&tab_sql[tab_idx], &tab_sql_len[tab_idx], tname, &qi->schema,
				pol_for_tab, pol_for_tab_len, n_pid, expected_list, unexpected_list,
				actual_list);
		for (i = 0; i < n_pid; i++) {
			if (pol_for_tab[i] && pol_for_tab_len[i]) {
				free(pol_for_tab[i]);
				pol_for_tab[i] = NULL;
				pol_for_tab_len[i] = 0;
			}
		}
	}

	*tab_sql_str = tab_sql;
	*tab_sql_str_len = tab_sql_len;
	*n_tab_sql = n_tab;

	if (pol_for_tab) {
		for (i = 0; i < n_pid; i++) {
			if (pol_for_tab[i] && pol_for_tab_len[i])
				free(pol_for_tab[i]);
		}
		free(pol_for_tab);
	}
	
	if (pol_for_tab_len)
		free(pol_for_tab_len);

	return 0;
}

