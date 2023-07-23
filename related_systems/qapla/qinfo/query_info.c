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
#include <sys/types.h>
#include <unistd.h>

// for extern "C" includes
#include "query_info.h"
#include "utils/format.h"
#include "common/config.h"
#include "common/db.h"
#include "common/query.h"
#include "common/qapla_policy.h"
#include "dbif/mysqlif.h"
#include "query_info_int.h"
#include "query_symbol.h"
#include "qapla_col_parser.h"
#include "policy_pool.h"
#include "pol_vector.h"
#include "metadata.h"
#include "pol_eval.h"
#include "pol_eval_col.h"
#include "pol_eval_mv.h"

#include "sql_pi/sql_rewrite.h"
//#include "hotcrp/hotcrp_pol_db.h"

extern void *init_parser(void);
extern void free_parser(void *);
extern int run_parser(void *p, void *qip, const char *text, size_t length, int is_utf8);
extern void *get_query_ast(void *p);

const char *sym_type_name[] = {"table", "column"};
char *
get_sym_type_name(int sym_type)
{
	if (sym_type < 0 || sym_type >= NUM_SYM_TYPE)
		return (char *) "";

	return (char *) sym_type_name[sym_type];
}

// == query info ==
void
_do_init_query_info(query_info_t *qinfo, const char *schema_name,
		const char *logname, int db_type, app_info_t *ai)
{
	if (!qinfo)
		return;

	qinfo->priv = malloc(sizeof(query_info_int_t));
	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	memset(qi, 0, sizeof(query_info_int_t));

	qi->db_type = db_type;
	qi->log_info.fname = strdup((char *) logname);
	FILE *f = fopen(qi->log_info.fname, "a+");
	qi->log_info.f = f;
	
	init_db(&qi->schema, schema_name, MAX_TAB, MAX_COL, ai->load_schema_fn);

	parser_context_t *pc = &qi->pc;
	init_parser_context(pc);
	pc->schema = &qi->schema;

	init_metadata(&qi->qm, &qi->schema, db_type);

#if CONFIG_POL_STORE == CONFIG_PSTORE_MEM
	if (ai->load_policies_fn)
		ai->load_policies_fn(&qi->schema, &qi->qm, ai->sql_pred_fns, ai->pol_fns, ai->col_fns);
#endif

	init_qstr(&qi->qstr);
	init_query_cache(&qi->qcache, QUERY_CACHE_SIZE);

	qi->parser = init_parser();
}

void
do_set_user_session(query_info_t *qinfo, char *sessid, char *email, char *pwd)
{
	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	init_session(&qi->session, email, 0);

  return;
}

int
qinfo_get_dbtype(query_info_t *qinfo)
{
	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	return qi->db_type;
}

void
setup_query_in_query_info(query_info_t *qinfo, const char *text, size_t length,
		int pe_method)
{
	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	qstr_set_orig_query(&qi->qstr, text, length, pe_method);
}

void
set_expected_or_unexpected_col_list(query_info_t *qinfo, char *cstr, int cstr_len,
		int expected)
{
	int ncol = 0;
	char *ptr = cstr;
	char *cptr;
	char *cstr_end = cstr + cstr_len;
	char *cstart = NULL, *cend = NULL, *tstart = NULL, *tend = NULL;
	char tname[MAX_NAME_LEN], cname[MAX_NAME_LEN];
	memset(tname, 0, MAX_NAME_LEN);
	memset(cname, 0, MAX_NAME_LEN);
	col_sym_t *cs = NULL;

	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	qstr_t *qstr = &qi->qstr;
	while (ptr < cstr_end) {
		cstart = ptr;
		tstart = ptr;
		cptr = ptr;
		while (cptr < cstr_end) {
			if (*cptr == '.') {
				tend = cptr;
				cstart = cptr + 1;
			}
			if (*cptr == ',' || (cptr == cstr_end - 1 && *cptr == ';')) {
				cend = cptr;
				break;
			}
			cptr++;
		}

		if (cend)
			strncpy(cname, cstart, cend-cstart);
		if (tend)
			strncpy(tname, tstart, tend-tstart);
		cs = alloc_init_col_sym_from_schema(&qi->schema, tname, cname);
#if DEBUG
		printf("added: %s.%s (%d.%d)\n", tname, cname, sym_field(cs, db_tid),
				sym_field(cs, db_cid));
#endif
		if (expected)
			qstr_add_expected_col(qstr, cs);
		else
			qstr_add_unexpected_col(qstr, cs);
		tend = cend = NULL;
		memset(tname, 0, MAX_NAME_LEN);
		memset(cname, 0, MAX_NAME_LEN);
		ptr = cptr + 1;
		if (*ptr == ' ') ptr++;
	}
}

int
do_eval_query_policies(query_info_t *qinfo)
{

	PVEC pv = 0;
	int ret = -1, ret2 = -1;
	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	int8_t eval_method = qstr_get_pol_eval_method(&qi->qstr);

#if DEBUG
    printf("evaluating query policies\n");
#endif

	list_t query_table_list;
	list_init(&query_table_list);

	int i;
	qapla_policy_t *pvec_qp[MAX_POLICIES];
	char *sql_str[MAX_POLICIES];
	int sql_str_len[MAX_POLICIES];
	int8_t processed[MAX_POLICIES];
	memset(pvec_qp, 0, sizeof(qapla_policy_t *) * MAX_POLICIES);
	memset(sql_str, 0, sizeof(char *) * MAX_POLICIES);
	memset(sql_str_len, 0, sizeof(int) * MAX_POLICIES);
	memset(processed, 0, sizeof(int8_t) * MAX_POLICIES);

	qapla_policy_t **pv_qp = NULL;
	int n_pvec_qp, n_pv_qp;

	char **tab_sql = NULL;
	int *tab_sql_len = NULL;
	int n_tab_sql = 0;

	// first check if aggr policy applicable
	ret = aggr_query_get_pvec(qi, &pv);
	if (ret < 0 && eval_method == PE_METHOD_ROW_SUPP) {
        // not an aggr query, get set of applicable policies
		ret2 = query_get_pvec(qi, &pv);
		if (ret2 < 0) {
#if DEBUG
            printf("not agg policy and pvec failed\n");
#endif
			return -1;
        }
	}

	if (ret == 0 || (ret2 == 0 && eval_method == PE_METHOD_ROW_SUPP)) {
		ret = query_pvec_get_policy_list(qi, pv, &pv_qp, &n_pv_qp);
		if (ret < 0)
			return -1;

		ret = get_resolved_sql_pol(qi, pv_qp, n_pv_qp, QP_PERM_READ, &tab_sql,
				&tab_sql_len, &n_tab_sql, &query_table_list);
		if (ret < 0)
			return -1;

		ret = sql_rewrite(qi, &query_table_list, tab_sql, tab_sql_len, n_tab_sql);

		goto cleanup;
	}

  // TODO: check ret < 0?
	if (eval_method == PE_METHOD_CELL_BLIND) {
#if DEBUG
        printf("Cell blinding mode on\n");
#endif
		ret = query_get_pid_for_each_col(qi, &query_table_list, pvec_qp, &n_pvec_qp);
		if (ret < 0) {
#if DEBUG
            printf("Getting pid for each col failed\n");
#endif
			return -1;
        }

		list_t *c_it;
		symbol_t *tsym;
		col_sym_t *ccs;
		qapla_policy_t *tmp_qp = NULL;
		uint16_t tmp_pid = 0;

		list_for_each_entry(tsym, &query_table_list, symbol_listp) {
			c_it = &tsym->col_sym_list;
			list_for_each_entry(ccs, c_it, col_sym_listp) {
				tmp_qp = (qapla_policy_t *) ccs->ptr;
				if (!tmp_qp) // for temp tables
					continue;

				tmp_pid = qapla_get_policy_id(tmp_qp);
				if (processed[tmp_pid])
					continue;

				ret = get_sql_clauses_in_pol(tmp_qp, qi, sym_field(tsym, db_tid),
						&sql_str[tmp_pid], &sql_str_len[tmp_pid]);
				processed[tmp_pid] = 1;
			}
		}

        ret = query_get_sql_for_table(qi, &query_table_list, (char **) sql_str,
            (int *) sql_str_len, &tab_sql, &tab_sql_len, &n_tab_sql);
        ret = sql_rewrite(qi, &query_table_list, tab_sql, tab_sql_len, n_tab_sql);
        goto cleanup;
	}

  // TODO: check ret < 0?
	if (eval_method == PE_METHOD_MAT_VIEW) {
		uint64_t tid_arr[10];
		int max_tab = 10;
		ret = query_get_table_list(qi, tid_arr, max_tab, &n_tab_sql);
		if (ret < 0)
			return -1;

		int i;
		uint64_t tid;
		char *tname = NULL;
		symbol_t *new_tsym = NULL;
		symbol_t *tsym = NULL;

		tab_sql = (char **) malloc(sizeof(char *) * n_tab_sql);
		tab_sql_len = (int *) malloc(sizeof(int) * n_tab_sql);
		memset(tab_sql, 0, sizeof(char *) * n_tab_sql);
		memset(tab_sql_len, 0, sizeof(int) * n_tab_sql);

		for (i = 0; i < n_tab_sql; i++) {
			tid = tid_arr[i];
			if ((int) tid < 0)
				continue; // for temporary tables

			tname = get_schema_table_name(&qi->schema, (int) tid);
#if DEBUG
			printf("%d. tid: %d, tname: %s\n", i, tid, tname);
#endif

			new_tsym = (symbol_t *) malloc(sizeof(symbol_t));
			init_symbol(new_tsym);
			set_sym_str_field(new_tsym, db_tname, tname);
			set_sym_field(new_tsym, db_tid, tid);
			list_insert_at_tail(&query_table_list, &new_tsym->symbol_listp);
		}

		i = 0;
		ret = query_segregate_cols_to_tables(qi, &query_table_list);
		//print_symbol_list(&query_table_list, qi->log_info.f);
		list_for_each_entry(tsym, &query_table_list, symbol_listp) {
#if DEBUG
			printf("tsym: %d: %s\n", sym_field(tsym, db_tid), sym_field(tsym, db_tname));
#endif
			get_mv_sql_for_table(qi, tsym, &tab_sql[i], &tab_sql_len[i]);
			i++;
		}
		ret = sql_rewrite(qi, &query_table_list, tab_sql, tab_sql_len, n_tab_sql);
		goto cleanup;
	}

cleanup:
	if (pv_qp && n_pv_qp) {
		free_policy_list(&pv_qp, n_pv_qp);
	}
	cleanup_symbol_list(&query_table_list);
	for (i = 0; i < MAX_POLICIES; i++) {
		if (sql_str[i])
			free(sql_str[i]);
	}
	if (tab_sql) {
		for (i = 0; i < n_tab_sql; i++) {
			if (tab_sql[i])
				free(tab_sql[i]);
		}
		free(tab_sql);
	}
	if (tab_sql_len)
		free(tab_sql_len);
	return ret;
}

void
get_rewritten_query_from_query_info(query_info_t *qinfo, char **q, int *qlen)
{
	int len;
	char *query;
	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	query = qstr_get_rewritten_query(&qi->qstr, &len);
	if (!len || !query)
		return;

	*q = query;
	*qlen = len;
}

void
do_reset_query_info(query_info_t *qinfo)
{
	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	reset_qstr(&qi->qstr);
	cleanup_parser_context(&qi->pc);
	qi->pc.schema = &qi->schema;
	if (qi->parser)
		free_parser(qi->parser);
	qi->parser = init_parser();
}

void
do_reset_query_cache(query_info_t *qinfo)
{
	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	reset_query_cache(&qi->qcache);
}

void
do_cleanup_query_info(query_info_t *qinfo)
{
	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	list_t *context_it, *next_context_it, *context_head;
	list_t *sym_it, *sym_head;
	list_t *cs_it, *cs_head;
	context_t *context;
	symbol_t *sym;
	col_sym_t *cs;

	free_parser(qi->parser);
	qi->parser = NULL;
	cleanup_db(&qi->schema);
	cleanup_metadata(&qi->qm);
	cleanup_qstr(&qi->qstr);
	cleanup_parser_context(&qi->pc);
	cleanup_query_cache(&qi->qcache);
	cleanup_session(&qi->session);
	if (qi->log_info.fname) {
		free(qi->log_info.fname);
		qi->log_info.fname = NULL;
	}

	if (qi->log_info.f) {
		fclose(qi->log_info.f);
		qi->log_info.f = NULL;
	}

	memset(qi, 0, sizeof(query_info_int_t));
	free(qinfo->priv);
	qinfo->priv = NULL;
}

void
log_query_info(query_info_t *qinfo, char *logstr)
{
	if (!qinfo || !logstr)
		return;

	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;

	FILE *f = fopen(qi->log_info.fname, "a+");
	if (!f)
		return;

	fprintf(f, "%s\n", logstr);
	//fprintf(f, "[%lu] %s\n", getpid(), logstr);

	fclose(f);
}

void
print_query_info(query_info_t *qinfo)
{
	if (!qinfo)
		return;

	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;

	FILE *f = fopen(qi->log_info.fname, "a+");
	if (!f)
		return;

	traverse_context_list_recursive(qi, print_context);
	fprintf(f, "\n\n");
	qstr_print_table_pos(&qi->qstr, f);

	fclose(f);
}

// == reference monitor code ==
int 
query_parser(query_info_t *qinfo, const char *text, size_t length,
		int is_utf8, int pe_method) 
{
	char *select_str = (char *) "select";
	int select_str_len = strlen(select_str);
	if (length < select_str_len)
		return -1;
	if (strncmp(text, select_str, select_str_len) != 0)
		return -1;
	STAT_START_TIMER(QRM_PARSER);
	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	STAT_START_TIMER(QRM_SQL_PARSER);
	run_parser(qi->parser, qi, text, length, is_utf8);
	STAT_END_TIMER(QRM_SQL_PARSER);
	STAT_START_TIMER(QRM_AST_RESOLVE);
	void *ast = get_query_ast(qi->parser);
	get_query_symbols(qi, ast);
	STAT_END_TIMER(QRM_AST_RESOLVE);
	//parse_query(qi, text, length, is_utf8);
	STAT_END_TIMER(QRM_PARSER);
	return 0;
}

int
run_refmon_no_cache(query_info_t *qinfo, const char *text, size_t length,
		int is_utf8, int pe_method, char **rewritten_q, int *rewritten_qlen)
{
	int ret = 0;
	char *new_q = NULL;
	int new_qlen = 0;

	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	setup_query_in_query_info(qinfo, text, length, pe_method);
	ret = query_parser(qinfo, text, length, is_utf8, pe_method);
	if (ret < 0)
		return ret;

	qstr_split_string_at_tables(&qi->qstr);

#if DEBUG
    printf("no cache going to eval query policies: %s\n", qi->qstr);
#endif

	STAT_START_TIMER(QRM_POL_EVAL);
	ret = do_eval_query_policies(qinfo);
	STAT_END_TIMER(QRM_POL_EVAL);

	if (ret < 0)
		return ret;

	list_t *qsplit_list = qstr_get_split_list(&qi->qstr);
	STAT_START_TIMER(QRM_RESOLVE_POL_PARMS);
	resolve_policy_parameters(qi, qsplit_list);

	STAT_END_TIMER(QRM_RESOLVE_POL_PARMS);
	gen_query(qsplit_list, NULL, &new_q, &new_qlen, qi->db_type);
	qstr_set_rewritten_query(&qi->qstr, new_q, new_qlen);

	*rewritten_q = new_q;
	*rewritten_qlen = new_qlen;
	return 0;
}

int
run_refmon_cache(query_info_t *qinfo, const char *text, size_t length,
		int is_utf8, int pe_method, char **rewritten_q, int *rewritten_qlen)
{
	int ret = 0;
	char *new_q = NULL;
	int new_qlen = 0;

	/*
	 * parameterize input query
	 * lookup cache with the parameterized query as key
	 * if found valid cache entry: 
	 * 	generate rewritten query from cached query elements list
	 * 	return generated query
	 * else:
	 * 	parse query
	 * 	apply policies
	 * 	rewrite query with policies
	 * 	translate syntax if required
	 * 	cache final query with parameterized orig query as key
	 * 	return final query
	 */
	char *p_buf = NULL;
	int p_len = 0;
	qcache_elem_t *cache_query = NULL;
	FILE *f = NULL;

	query_info_int_t *qi = (query_info_int_t *) qinfo->priv;
	setup_query_in_query_info(qinfo, text, length, pe_method);
	qstr_set_first_split_string(&qi->qstr);
	STAT_START_TIMER(QRM_SCANNER);
	qstr_parameterize_split_string(&qi->qstr);
	STAT_END_TIMER(QRM_SCANNER);
	
#if DEBUG
    printf("going to eval query policies cache\n");
#endif

	qstr_gen_parameterized_query(&qi->qstr, &p_buf, &p_len);
	assert(p_buf && p_len);
#if DEBUG
	printf(" -------- PBUF (%d) -------\n%s\n", p_len, p_buf);
#endif
	
	STAT_START_TIMER(QRM_CACHE_GET);
	cache_query = (qcache_elem_t *) query_cache_get_entry(&qi->qcache, 
			(void *) p_buf, p_len, 1);
	STAT_END_TIMER(QRM_CACHE_GET);

	if (cache_query) {
#if DEBUG
		printf("found cached entry\n");
#endif

		list_t *rw_q_list = NULL;
		
		STAT_START_TIMER(QRM_CACHE_REWRITE);

		list_t clone_list;
		list_init(&clone_list);
		list_t *qsplit_list = qstr_get_split_list(&qi->qstr);
#if CONFIG_REFMON == RM_CACHE_PARAM_POLICY
		STAT_START_TIMER(QRM_CLONE_CACHE_ELEM);
		clone_qelem_list(&cache_query->rw_q_list, &clone_list);
		STAT_END_TIMER(QRM_CLONE_CACHE_ELEM);

		STAT_START_TIMER(QRM_RESOLVE_POL_PARMS);
		resolve_policy_parameters(qi, &clone_list);
		STAT_END_TIMER(QRM_RESOLVE_POL_PARMS);
		rw_q_list = &clone_list;

#else
		rw_q_list = &cache_query->rw_q_list;
#endif
		
		gen_query(rw_q_list, qsplit_list, &new_q, &new_qlen, qi->db_type);
		// set this in qstr, so that it can be freed
		qstr_set_rewritten_query(&qi->qstr, new_q, new_qlen);
		STAT_END_TIMER(QRM_CACHE_REWRITE);

#if CONFIG_REFMON == RM_CACHE_PARAM_POLICY
		cleanup_qsplit_list(&clone_list);
#endif

	} else {
#if DEBUG
		printf("no cached entry\n");
#endif
		reset_query_in_qstr(&qi->qstr);

		ret = query_parser(qinfo, text, length, is_utf8, pe_method);
		if (ret < 0)
			return ret;

		qstr_split_string_at_tables(&qi->qstr);

		STAT_START_TIMER(QRM_POL_EVAL);
		ret = do_eval_query_policies(qinfo);
		STAT_END_TIMER(QRM_POL_EVAL);

#if DEBUG
		printf("EVAL POL: %d\n", ret);
#endif
		if (ret < 0)
			return ret;

		STAT_START_TIMER(QRM_SCANNER);
		qstr_parameterize_split_string(&qi->qstr);
		STAT_END_TIMER(QRM_SCANNER);

		list_t *rw_q_list;
		list_t *qsplit_list = qstr_get_split_list(&qi->qstr);
#if (CONFIG_REFMON == RM_CACHE_PARAM_POLICY)
		/* 
		 * insert parameterized rewritten query in cache, 
		 * keyed by parameterized orig query,
		 * parameters of orig query + parmeterized policies
		 */
		STAT_START_TIMER(QRM_CLONE_QSTR_ELEM);
		list_t clone_list;
		list_init(&clone_list);
		clone_qelem_list(qsplit_list, &clone_list);
		STAT_END_TIMER(QRM_CLONE_QSTR_ELEM);
		rw_q_list = &clone_list;
#else
		rw_q_list = qsplit_list;
#endif
		STAT_START_TIMER(QRM_CACHE_SET);
		qcache_elem_t *qce = init_qcache_elem(p_buf, p_len, rw_q_list);
		query_cache_add_entry(&qi->qcache, qce);
		STAT_END_TIMER(QRM_CACHE_SET);

#if (CONFIG_REFMON == RM_CACHE_PARAM_POLICY)
		STAT_START_TIMER(QRM_RESOLVE_POL_PARMS);
		resolve_policy_parameters(qi, qsplit_list);
		STAT_END_TIMER(QRM_RESOLVE_POL_PARMS);
#endif

		STAT_START_TIMER(QRM_REWRITE);
#if CONFIG_REFMON == RM_CACHE_PARAM_POLICY
		rw_q_list = qsplit_list;
#else
		rw_q_list = &qce->rw_q_list;
#endif
		gen_query(rw_q_list, NULL, &new_q, &new_qlen, qi->db_type);
		qstr_set_rewritten_query(&qi->qstr, new_q, new_qlen);
		STAT_END_TIMER(QRM_REWRITE);
	}

	if (p_buf)
		free(p_buf);
	p_buf = NULL;

	*rewritten_q = new_q;
	*rewritten_qlen = new_qlen;
	return 0;
}

void
get_query_symbols(void *qip, void *ast)
{
	do_traverse_ast_resolve(qip, ast);
}

typedef int (*do_func)(void *qi, void *data);

int 
print_context(void *qip, void *data)
{
	query_info_int_t *qi = (query_info_int_t *) qip;
	FILE *f = qi->log_info.f;
	if (!f)
		return -1;

	context_t *context = (context_t *) data;
	fprintf(f, "--start--\n\n");
	fprintf(f, "context%d\n", context->id);
	int i;
	for (i = 0; i < NUM_SYM_TYPE; i++) {
		fprintf(f, "LIST %s:\n", get_sym_type_name(i));
		print_symbol_list(&context->symbol_list[i], f);
		fprintf(f, "\n");
	}
	if (context->id == 0) {
		fprintf(f, "is aggr query: %d\n", context->is_aggr_query);
	}
	fprintf(f, "--end--\n\n");

	return 0;
}

void 
traverse_context_list(void *qip, do_func fn)
{
	query_info_int_t *qi = (query_info_int_t *) qip;
	parser_context_t *pc = &qi->pc;
	list_t *context_head = &pc->context_stack.list;
	list_t *context_it = context_head->next;
	while (context_it != context_head) {
		context_t *context = list_entry(context_it, context_t, context_listp);
		context_it = context_it->next;
		fn(qip, (void *) context);
	}
}

void 
traverse_context_recursive(void *qip, context_t *context, do_func fn)
{
	context->visited = 1;
	int n_child_context = context->child_context_idx;
	int i = 0;
	for (i = 0; i < n_child_context; i++) {
		traverse_context_recursive(qip, context->children[i], fn);
	}
	fn(qip, (void *) context);
}

void 
traverse_context_list_recursive(void *qip, do_func fn)
{
	query_info_int_t *qi = (query_info_int_t *) qip;
	parser_context_t *pc = &qi->pc;
	list_t *context_head = &pc->context_stack.list;
	list_t *context_it = context_head->next;
	while (context_it != context_head) {
		context_t *context = list_entry(context_it, context_t, context_listp);
		context_it = context_it->next;

		if (!context->visited)
			traverse_context_recursive(qip, context, fn);
	}
}

int 
reset_context_visited(void *qip, void *data)
{
	context_t *context = (context_t *) data;
	context->visited = 0;

	return 0;
}
