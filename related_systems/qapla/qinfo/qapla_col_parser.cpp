/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#include "mysqlparser/MySQLLexer.h"
#include "utils/list.h"
#include "common/db.h"
#include "query_info_int.h"
#include "query_symbol.h"
#include "qapla_col_parser.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

void scan_spl_query(query_info_int_t *qi, pANTLR3_BASE_TREE tree, 
		int start_idx, int end_idx);
void scan_select_expr_tok(query_info_int_t *qi, pANTLR3_BASE_TREE tree);
void scan_where_expr(query_info_int_t *qi, pANTLR3_BASE_TREE tree, 
		ANTLR3_UINT32 *cidx, int expr_type);
void scan_table_expr(query_info_int_t *qi, pANTLR3_BASE_TREE tree, 
		ANTLR3_UINT32 start_idx, ANTLR3_UINT32 *end_idx);
void scan_join_expr(query_info_int_t *qi, pANTLR3_BASE_TREE tree, 
		symbol_t *tab1_sym);
void scan_table_ref(query_info_int_t *qi, pANTLR3_BASE_TREE tree, 
		ANTLR3_UINT32 *idx, symbol_t **sym);
void scan_runtime_function(query_info_int_t *qi, pANTLR3_BASE_TREE tree, 
		list_t *xf_list, int expr_type, char *expr_alias);
void scan_expr_tok(query_info_int_t *qi, pANTLR3_BASE_TREE tree, list_t *xf_list, 
		int expr_type, char *expr_alias);
void scan_spl_expr_tok(query_info_int_t *qi, pANTLR3_BASE_TREE tree,
		int expr_type, char *expr_alias);
void scan_subquery(query_info_int_t *qi, pANTLR3_BASE_TREE parent, int from, 
		ANTLR3_UINT32 *cidx, symbol_t **tab_sym);
void scan_col_ref(query_info_int_t *qi, pANTLR3_BASE_TREE tree, list_t *xf_list, 
		int expr_type, char *col_alias);
void traverse_ast_select(void *qip, pANTLR3_BASE_TREE tree, 
		ANTLR3_UINT32 *idx, int from_subquery);

void 
scan_col_ref(query_info_int_t *qi, pANTLR3_BASE_TREE tree, list_t *xf_list,
		int expr_type, char *col_alias)
{
	pANTLR3_BASE_TREE child[2];
	pANTLR3_COMMON_TOKEN tok[2];
	pANTLR3_STRING tok_text[2];
	ANTLR3_UINT32 tok_type[2], num_children;
	char *tname = NULL, *cname = NULL;

	context_t *currC = NULL;
	symbol_t *lookup_sym = NULL;
	num_children = tree->getChildCount(tree);
	assert(num_children == 1 || num_children == 3);

	child[0] = (pANTLR3_BASE_TREE) tree->getChild(tree, (num_children - 1));
	tok_text[0] = child[0]->getText(child[0]);
	cname = (char *) tok_text[0]->chars;
	tok[0] = child[0]->getToken(child[0]);
	tok_type[0] = tok[0]->getType(tok[0]);
	assert(tok_type[0] == IDENTIFIER || tok_type[0] == MULT_OPERATOR);

	if (num_children == 3) {
		child[1] = (pANTLR3_BASE_TREE) tree->getChild(tree, 0);
		tok_text[1] = child[1]->getText(child[1]);
		tname = (char *) tok_text[1]->chars;
		tok[1] = child[1]->getToken(child[1]);
		tok_type[1] = tok[1]->getType(tok[1]);
		assert(tok_type[1] == IDENTIFIER || tok_type[1] == MULT_OPERATOR);
	}

	char buf[MAX_NAME_LEN], buf2[MAX_NAME_LEN];
	memset(buf, 0, MAX_NAME_LEN);
	memset(buf2, 0, MAX_NAME_LEN);
	
	if (col_alias)
		sprintf(buf, "%s", col_alias);
	else if (num_children == 1)
		sprintf(buf, "%s", cname);
	else
		sprintf(buf, "%s.%s", tname, cname);

	if (!col_alias)
		get_xform_list(buf2, NULL, xf_list, buf);
	else
		sprintf(buf2, "%s", buf);

	/* 
	 * inserting symbol with col_sym into current context 
	 */
	parser_context_t *pc = &qi->pc;
	currC = get_curr_context(pc);
	lookup_sym = get_symbol_by_name_in_context_list(currC, buf2, SYM_COL);
	int insert = 0;
	if (!lookup_sym) {
		lookup_sym = alloc_init_symbol(pc, buf2, SYM_COL);
		insert_sym_to_context(currC, lookup_sym);
	}

	// insert col_sym after resolving it, in order to avoid adding duplicates to col_sym_list
	insert_resolved_col_sym_to_sym_list(pc, lookup_sym, tname, cname, xf_list, 
			expr_type);
}

void 
scan_subquery(query_info_int_t *qi, pANTLR3_BASE_TREE parent, int from, 
		ANTLR3_UINT32 *cidx, symbol_t **tab_sym)
{
	if (!from) {
		ANTLR3_UINT32 nidx = *cidx; 
		/* 
		 * subquery token, appears under where expression symbol
		 * parent = expr; parent->child[nidx] = subquery; 
		 * subquery->child[0] = select,from...
		 */
		traverse_ast_select(qi, parent, &nidx, 0);
		parser_context_t *pc = &qi->pc;
		context_t *currC = get_curr_context(pc);

		list_t dup_table_list;
		list_init(&dup_table_list);
		dup_context_symbol_list(currC, SYM_TAB, &dup_table_list);

		list_t dup_col_list;
		list_init(&dup_col_list);
		dup_context_symbol_list(currC, SYM_COL, &dup_col_list);

		pc->context_stack.tos = currC->prev_tos;
		currC->prev_tos = NULL;
		currC = get_curr_context(pc);

		symbol_t *tt_sym = NULL;
		list_t *sym_it = NULL, *sym_head = NULL, *sym_next = NULL;
		/*
		 * insert the table of the sub-query into the context of outer query too,
		 * but at the end of the list, so that symbols in outer query first search
		 * in the tables of the outer context only
		 */
		sym_head = &dup_table_list;
		sym_it = sym_head->next;
		while (sym_it != sym_head) {
			tt_sym = list_entry(sym_it, symbol_t, symbol_listp);
			sym_next = sym_it->next;
			list_remove(sym_it);
			list_insert_at_tail(&currC->symbol_list[SYM_TAB], sym_it);
			sym_it = sym_next;
		}

		/*
		 * insert symbols from sub-query into the context of outer query
		 */
		sym_head = &dup_col_list;
		sym_it = sym_head->next;
		while (sym_it != sym_head) {
			tt_sym = list_entry(sym_it, symbol_t, symbol_listp);
			sym_next = sym_it->next;
			list_remove(sym_it);
			list_insert(&currC->symbol_list[SYM_COL], sym_it);
			sym_it = sym_next;
		}

	} else {
		/* 
		 * no subquery token, appears adjacent to from/join symbol
		 * parent = from/join; parent->child[nidx] = select,from...
		 */
		context_t *currC = NULL, *newC = NULL, *parentC = NULL;
		char *alias = NULL;
		int end = 0;
		pANTLR3_BASE_TREE child, next;
		pANTLR3_COMMON_TOKEN tok;
		ANTLR3_UINT32 tok_type, nidx, num_children, i;
		pANTLR3_STRING tok_text;
		i = nidx = *cidx;
		
		num_children = parent->getChildCount(parent);
		do {
			next = (pANTLR3_BASE_TREE) parent->getChild(parent, i);
			if (!next)
				break;

			tok = next->getToken(next);
			tok_type = tok->getType(tok);
			switch (tok_type) {
				case SELECT_SYMBOL:
					nidx = i;
					traverse_ast_select(qi, parent, &nidx, 1);
					i = nidx;
					break;
				case WHERE_SYMBOL:
				case GROUP_SYMBOL:
				case HAVING_SYMBOL:
				case JOIN_EXPR_TOKEN:
				case ON_SYMBOL:
				case USING_SYMBOL:
				case ORDER_SYMBOL:
				case CLOSE_PAR_SYMBOL:
				case LIMIT_SYMBOL:
				case IDENTIFIER: // if no as
					end = 1;
					i--;
					break;
			}
			if (!end)
				i++;
		} while (!end);

		parser_context_t *pc = &qi->pc;
		currC = get_curr_context(pc);

		/*
		 * list of projected columns in the from/join subquery constitutes the
		 * schema of the temporary table. extract the resolved col info and add
		 * to the alias symbol in the context and db of the outer query
		 */
		// copy all (not just projected) symbols from subcontext into outermost context
		list_t temp_table_sym_list;
		list_init(&temp_table_sym_list);
		dup_context_symbol_list(currC, SYM_COL, &temp_table_sym_list);

		list_t dup_context_list;
		list_init(&dup_context_list);
		dup_context_symbol_list(currC, SYM_COL, &dup_context_list);

		/*
		 * pop the context of the subquery
		 */
		pc->context_stack.tos = currC->prev_tos;
		currC->prev_tos = NULL;

		if (i >= num_children)
			return;

		end = 0;
		for (; i < num_children; i++) {
			next = (pANTLR3_BASE_TREE) parent->getChild(parent, i);
			tok = next->getToken(next);
			tok_type = tok->getType(tok);
			switch (tok_type) {
				case AS_SYMBOL: // even this will not show up, directly alias identifier
						next = (pANTLR3_BASE_TREE) parent->getChild(parent, i+1);
						tok = next->getToken(next);
						tok_type = tok->getType(tok);
						assert(tok_type == IDENTIFIER);
						i++;
					case IDENTIFIER:
						tok_text = next->getText(next);
						alias = (char *) tok_text->chars;
						break;
					case ON_SYMBOL:
					case WHERE_SYMBOL:
					case GROUP_SYMBOL:
					case HAVING_SYMBOL:
					case JOIN_EXPR_TOKEN:
					case LIMIT_SYMBOL:
						end = 1;
						i--;
						break;
			}
			if (end)
				break;
		}

		/*
		 * add new symbol for temp table alias into context of outer query
		 */
		int insert = 0, db_tid = -1, db_cid = -1;
		currC = get_curr_context(pc);
		symbol_t *lookup_sym = get_symbol_by_name_in_context_list(currC, alias, SYM_TAB);
		if (!lookup_sym) {
			lookup_sym = alloc_init_symbol(pc, alias, SYM_TAB);
			insert_sym_to_context(currC, lookup_sym);
		}

		/* 
		 * alias for a temp table in from/join subquery -- insert list of projected 
		 * cols in subquery into col_sym_list of the new symbol
		 */
		col_sym_t *cs = NULL;
		symbol_t *tt_sym = NULL;
		list_t *sym_it = NULL, *sym_head = NULL, *sym_next = NULL;
		list_t *cs_it = NULL, *cs_head = NULL, *next_cs_it = NULL;
		sym_head = &temp_table_sym_list;
		sym_it = sym_head->next;
		while (sym_it != sym_head) {
			tt_sym = list_entry(sym_it, symbol_t, symbol_listp);
			sym_next = sym_it->next;
			cs_head = &tt_sym->col_sym_list;
			cs_it = cs_head->next;
			while (cs_it != cs_head) {
				cs = list_entry(cs_it, col_sym_t, col_sym_listp);
				next_cs_it = cs_it->next;

				// this should only happen in the case of special query constructs.
				// update 06/07/16: now the symbols get default table name of "dual"
				if (!sym_field(cs, db_tname)) {
					set_sym_str_field(cs, db_tname, (char *) DUAL_TNAME);
					set_sym_field(cs, db_tid, DUAL_TID);
				}
				list_remove(cs_it);
				insert_col_sym_to_sym_list(lookup_sym, cs);
				cs_it = next_cs_it;
			}
			list_remove(sym_it);
			free_symbol(&tt_sym);
			sym_it = sym_next;
		}

		/*
		 * also insert symbols from the sub-query into the outer context directly
		 */
		sym_head = &dup_context_list;
		sym_it = sym_head->next;
		while (sym_it != sym_head) {
			tt_sym = list_entry(sym_it, symbol_t, symbol_listp);
			sym_next = sym_it->next;
			list_remove(sym_it);
			list_insert(&currC->symbol_list[SYM_COL], sym_it);
			sym_it = sym_next;
		}

		*cidx = i;
		if (tab_sym)
			*tab_sym = lookup_sym;
	}
}

void 
scan_spl_expr_tok(query_info_int_t *qi, pANTLR3_BASE_TREE tree,
		int expr_type, char *expr_alias)
{
	pANTLR3_BASE_TREE child, child2;
	pANTLR3_COMMON_TOKEN tok;
	pANTLR3_STRING tok_text;
	ANTLR3_UINT32 i, tok_type, num_children, num_children2, nidx = 0;
	num_children = tree->getChildCount(tree);

	for (i = 0; i < num_children; i++) {
		child = (pANTLR3_BASE_TREE)tree->getChild(tree, i);
		tok = child->getToken(child);
		tok_type = tok->getType(tok);
		switch(tok_type) {
			case STRING_TOKEN:
				{
					num_children2 = child->getChildCount(child);
					child2 = (pANTLR3_BASE_TREE)child->getChild(child, num_children2-1);
					tok = child2->getToken(child2);
					tok_type = tok->getType(tok);
					assert(tok_type == SINGLE_QUOTED_TEXT);
					child = child2;
					// fall through for subsequent handling of the constant, just as in int
				}
			case INT_NUMBER:
				{
					if (expr_alias) {
						tok_text = child->getText(child);
						char *numstr = (char *) tok_text->chars;
						parser_context_t *pc = &qi->pc;
						context_t *currC = get_curr_context(pc);
						symbol_t *sym = get_symbol_by_name_in_context_list(currC, expr_alias, SYM_COL);
						if (!sym) {
							sym = alloc_init_symbol(pc, expr_alias, SYM_COL);
							insert_sym_to_context(currC, sym);
						}

						char *tname = NULL;
						int ctxdb_tid = 
							get_schema_tid_for_context_col_sym(currC, pc->schema, expr_alias);
						if (ctxdb_tid >= 0)
							tname = get_schema_table_name(pc->schema, ctxdb_tid);
						else {
							ctxdb_tid = DUAL_TID;
							tname = (char *) DUAL_TNAME;
						}
						//char *tname = (char *) DUAL_TNAME;
						list_t int_xf_list;
						list_init(&int_xf_list);
						col_sym_t *cs = alloc_init_col_sym(NULL, tname, expr_alias, &int_xf_list, expr_type);
						set_sym_field(cs, db_tid, ctxdb_tid);
						col_sym_t *inserted = NULL;
						inserted = insert_col_sym_to_sym_list(sym, cs);
						if (inserted != cs) {
							free_col_sym(&cs);
							cs = inserted;
						}
					}
				}
				break;
		}
	}
}

void
scan_subquery_expr_first(query_info_int_t *qi, pANTLR3_BASE_TREE tree, int *num_subq, 
		int **subq_start_idx, int **subq_end_idx)
{
	pANTLR3_BASE_TREE child, child2;
	pANTLR3_COMMON_TOKEN tok;
	ANTLR3_UINT32 i, tok_type, num_children, nidx = 0;
	num_children = tree->getChildCount(tree);
	int subq_iter = 0;
	int *start_idx = NULL;
	int *end_idx = NULL;

	start_idx = (int *) malloc(sizeof(int) * num_children);
	end_idx = (int *) malloc(sizeof(int) * num_children);
	for (i = 0; i < num_children; i++) {
		start_idx[i] = -1;
		end_idx[i] = -1;
	}

	for (i = 0; i < num_children; i++) {
		child = (pANTLR3_BASE_TREE) tree->getChild(tree, i);
		tok = child->getToken(child);
		tok_type = tok->getType(tok);
		switch (tok_type) {
			case SUBQUERY_TOKEN:
				nidx = i;
				start_idx[subq_iter] = nidx;
				scan_subquery(qi, tree, 0, &nidx, NULL);
				i = nidx;
				end_idx[subq_iter] = nidx;
				subq_iter++;
				(*num_subq)++;
				break;
		}
	}

	*subq_start_idx = start_idx;
	*subq_end_idx = end_idx;
	return;
}

void 
scan_expr_tok(query_info_int_t *qi, pANTLR3_BASE_TREE tree, list_t *xf_list,
		int expr_type, char *expr_alias)
{
	pANTLR3_BASE_TREE child, child2;
	pANTLR3_COMMON_TOKEN tok;
	ANTLR3_UINT32 i, tok_type, num_children, nidx = 0;
	num_children = tree->getChildCount(tree);
	int num_subq = 0, subq_iter = 0;
	int *subq_start_idx = NULL, *subq_end_idx = NULL;

	scan_subquery_expr_first(qi, tree, &num_subq, &subq_start_idx, &subq_end_idx);

	for (i = 0; i < num_children; i++) {
		if (num_subq) {
			// we have already visited the subquery child nodes
			if (i >= subq_start_idx[subq_iter] && i <= subq_end_idx[subq_iter]) {
				i = subq_end_idx[subq_iter];
				subq_iter++;
				continue;
			}
		}
		child = (pANTLR3_BASE_TREE)tree->getChild(tree, i);
		tok = child->getToken(child);
		tok_type = tok->getType(tok);
		switch(tok_type) {
			case COLUMN_REF_TOKEN: 
				scan_col_ref(qi, child, xf_list, expr_type, expr_alias);
				break;

			case EQUAL_OPERATOR:
			case GREATER_THAN_OPERATOR:
			case GREATER_OR_EQUAL_OPERATOR:
			case LESS_THAN_OPERATOR:
			case LESS_OR_EQUAL_OPERATOR:
			case NOT_EQUAL_OPERATOR:
			case BITWISE_AND_OPERATOR:
			case BITWISE_OR_OPERATOR:
			case BITWISE_XOR_OPERATOR:
			case PLUS_OPERATOR:
			case MINUS_OPERATOR:
			case MULT_OPERATOR:
			case DIV_OPERATOR:
			case MOD_OPERATOR:
			case IS_SYMBOL:
			case LIKE_SYMBOL:
			case AND_SYMBOL:
			case OR_SYMBOL:
			case PAR_EXPRESSION_TOKEN:
			case EXPRESSION_TOKEN:
			case NOT_SYMBOL:
				scan_expr_tok(qi, child, xf_list, expr_type, expr_alias);
				break;

			case WHEN_SYMBOL:
			case THEN_SYMBOL:
			case ELSE_SYMBOL:
				child2 = (pANTLR3_BASE_TREE) child->getChild(child, 0);
				tok = child2->getToken(child2);
				tok_type = tok->getType(tok);
				assert(tok_type == EXPRESSION_TOKEN);
				scan_expr_tok(qi, child2, xf_list, expr_type, expr_alias);
				break;

			case SUBQUERY_TOKEN:
				nidx = i;
				scan_subquery(qi, tree, 0, &nidx, NULL);
				i = nidx;
				break;
		
			case UDF_CALL_TOKEN:
			case RUNTIME_FUNCTION_TOKEN:
				scan_runtime_function(qi, child, xf_list, expr_type, expr_alias);
				break;

			case INT_NUMBER:
			case STRING_TOKEN:
				scan_spl_expr_tok(qi, tree, expr_type, expr_alias);
				break;
		}
	}

	if (subq_start_idx)
		free(subq_start_idx);
	if (subq_end_idx)
		free(subq_end_idx);
	subq_start_idx = subq_end_idx = NULL;
}

void 
scan_runtime_function(query_info_int_t *qi, pANTLR3_BASE_TREE tree, list_t *xf_list,
		int expr_type, char *expr_alias)
{
	pANTLR3_BASE_TREE child;
	pANTLR3_COMMON_TOKEN tok;
	ANTLR3_UINT32 tok_type, num_children, i;
	transform_t *tf;
	int function = -1;

	num_children = tree->getChildCount(tree);
	for (i = 0; i < num_children; i++) {
		child = (pANTLR3_BASE_TREE) tree->getChild(tree, i);
		tok = child->getToken(child);
		tok_type = tok->getType(tok);
		switch (tok_type) {
			case COUNT_SYMBOL: function = COUNT;
				break;
			case SUM_SYMBOL: function = SUM;
				break;
			case AVG_SYMBOL: function = AVG;
				break;
			case STD_SYMBOL: function = STD;
				break;
			case MAX_SYMBOL: function = MAX;
				break;
			case MIN_SYMBOL: function = MIN;
				break;

			case MULT_OPERATOR: // count(*)
				{
					char *str = (char *) "*";
					col_sym_t *cs = NULL, *inserted = NULL;
					char *dbtname, *dbcname;
					int db_tid;
					char buf[MAX_NAME_LEN];
					memset(buf, 0, MAX_NAME_LEN);
					generate_col_sym_alias(buf, NULL, str, xf_list);
					parser_context_t *pc = &qi->pc;
					context_t *currC = get_curr_context(pc);
					symbol_t *sym = get_symbol_by_name_in_context_list(currC, buf, SYM_COL);
					char *sym_name = (expr_alias ? : buf);
					if (!sym) {
						sym = alloc_init_symbol(pc, sym_name, SYM_COL);
						insert_sym_to_context(currC, sym);
					}

					list_t *sym_head = &currC->symbol_list[SYM_TAB];
					symbol_t *sym2;
					list_t dup_xf_list;
					list_init(&dup_xf_list);
					list_for_each_entry(sym2, sym_head, symbol_listp) {
						dbtname = sym_field(sym2, db_tname);
						if (dbtname) {
							db_tid = get_schema_table_id(&qi->schema, dbtname);
							//instead of storing *, store any one (eg. 0) of the cols name as cname
							dbcname = get_schema_col_name_in_table_id(&qi->schema, db_tid, 0);
							dup_transform(&dup_xf_list, xf_list);
							//cs = alloc_init_col_sym(sym_name, dbtname, str, &dup_xf_list, expr_type);
							cs = alloc_init_col_sym(sym_name, dbtname, dbcname, 
									&dup_xf_list, expr_type);
							inserted = insert_col_sym_to_sym_list(sym, cs);
							if (inserted != cs) {
								free_col_sym(&cs);
								cs = inserted;
							}
						}
					}
					// we already transfer transformations of count(*) to a column of the 
					// schema, free up the xf_list here
					free_transform_list(xf_list);
				}
				break;

			case COALESCE_SYMBOL:
				 {
					 i++;
					 child = (pANTLR3_BASE_TREE) tree->getChild(tree, i);
					 tok = child->getToken(child);
					 tok_type = tok->getType(tok);
					 assert(tok_type == PAR_EXPRESSION_TOKEN);
					 // fall-through to handle it like expression token
				 }
			case EXPRESSION_TOKEN:
				scan_expr_tok(qi, child, xf_list, expr_type, expr_alias);
			 break;

			case IDENTIFIER:
				{
					pANTLR3_COMMON_TOKEN p_tok = tree->getToken(tree);
					ANTLR3_UINT32 p_tok_type = p_tok->getType(p_tok);
					if (p_tok_type != UDF_CALL_TOKEN)
						break;
					
					pANTLR3_STRING tok_text = child->getText(child);
					char *udf_name = (char *) tok_text->chars;
					if (strcmp(udf_name, "affil") == 0)
						function = AFFIL;
					else if (strcmp(udf_name, "topic") == 0)
						function = TOPIC;
					else
						function = -1; // unrecognized udf
				}
				break;
		}

		if (function == COUNT || function == SUM || function == AVG) {
			parser_context_t *pc = &qi->pc;
			context_t *firstC = get_first_context(pc);
			firstC->is_aggr_query = 1;
		}

		if (function >= 0) {
			tf = init_transform(function);
			list_insert(xf_list, &tf->xform_listp);
			function = -1;
		}
	}
}

void 
scan_table_ref(query_info_int_t *qi, pANTLR3_BASE_TREE tree, ANTLR3_UINT32 *idx, 
		symbol_t **sym)
{
	pANTLR3_BASE_TREE next, child;
	pANTLR3_COMMON_TOKEN tok;
	pANTLR3_STRING tok_text;
	ANTLR3_UINT32 num_children, tok_type, i;
	char *tname = NULL, *alias = NULL;

	int qpos_off = 0, qpos_len = 0, qpos_line = 0;
	int qpos_alias_off = 0, qpos_alias_len = 0, qpos_alias_line = 0;

	i = *idx;
	num_children = tree->getChildCount(tree);
	child = (pANTLR3_BASE_TREE) tree->getChild(tree, i);
	tok = child->getToken(child);
	tok_type = tok->getType(tok);
	assert(tok_type == TABLE_REF_TOKEN);
	next = (pANTLR3_BASE_TREE) child->getChild(child, 0);
	tok = next->getToken(next);
	tok_type = tok->getType(tok);
	assert(tok_type == IDENTIFIER);
	tok_text = tok->getText(tok);
	tname = (char *) tok_text->chars;

	qpos_off = (int) next->getCharPositionInLine(next);
	qpos_len = tok->stop - tok->start + 1;
	qpos_line = (int) next->getLine(next);

	int noalias = 0;
	if (i+1 >= num_children)
		noalias |= 1;
	else {
		next = (pANTLR3_BASE_TREE) tree->getChild(tree, i+1);
		tok = next->getToken(next);
		tok_type = tok->getType(tok);
		switch (tok_type) {
			case JOIN_EXPR_TOKEN:
			case WHERE_SYMBOL:
			case GROUP_SYMBOL:
			case HAVING_SYMBOL:
			case ON_SYMBOL:
			case USING_SYMBOL:
			case ORDER_SYMBOL:
			case CLOSE_PAR_SYMBOL:
			case LIMIT_SYMBOL:
				noalias |= 1;
				break;

			case AS_SYMBOL:
				next = (pANTLR3_BASE_TREE) tree->getChild(tree, i+2);
				tok = next->getToken(next);
				tok_type = tok->getType(tok);
				assert(tok_type == IDENTIFIER);
				i++;
			case IDENTIFIER:
				tok_text = next->getText(next);
				alias = (char *) tok_text->chars;

				qpos_alias_off = (int) next->getCharPositionInLine(next);
				qpos_alias_len = tok->stop - tok->start + 1;
				qpos_alias_line = (int) next->getLine(next);
				i++;
				break;
		}
	}

	symbol_t *lookup_sym = NULL;
	char *sym_name = (noalias ? tname : alias);
	parser_context_t *pc = &qi->pc;
	context_t *currC = get_curr_context(pc);
	lookup_sym = get_symbol_by_name_in_context_list(currC, sym_name, SYM_TAB);
	if (!lookup_sym) {
		lookup_sym = alloc_init_symbol(pc, sym_name, SYM_TAB);
		// setting db_tname, tid in symbol distinguishes db tables from tmp tables
		int tid = get_schema_table_id(pc->schema, tname);
		set_sym_str_field(lookup_sym, db_tname, tname);
		set_sym_field(lookup_sym, db_tid, tid);
		insert_sym_to_context(currC, lookup_sym);
	}

	list_t tab_xf_list;
	list_init(&tab_xf_list);
	col_sym_t *cs = alloc_init_col_sym(sym_name, tname, NULL, &tab_xf_list, TABLE);
	col_sym_t *inserted = NULL;
	inserted = insert_col_sym_to_sym_list(lookup_sym, cs);
	if (inserted != cs) {
		free_col_sym(&cs);
		cs = inserted;
	}

	*idx = i;
	if (sym)
		*sym = lookup_sym;

	qstr_add_table_pos(&qi->qstr, qpos_off, qpos_len, qpos_line, qpos_alias_off,
			qpos_alias_len, qpos_alias_line, &lookup_sym->s);
}

void 
scan_join_expr(query_info_int_t *qi, pANTLR3_BASE_TREE tree, symbol_t *tab1_sym)
{
	pANTLR3_BASE_TREE child, next;
	pANTLR3_COMMON_TOKEN tok;
	pANTLR3_STRING tok_text;
	ANTLR3_UINT32 num_children, tok_type, i, start_tab_idx, start_sub_idx, start_join_idx;
	symbol_t *tab2_sym = NULL;
	col_sym_t *cs = NULL, *inserted = NULL;
	list_t join_xf_list;
	list_init(&join_xf_list);
	char *name = NULL;
	int db_tid = 0, db_cid = 0;

	num_children = tree->getChildCount(tree);
	for (i = 0; i < num_children; i++) {
		child = (pANTLR3_BASE_TREE) tree->getChild(tree,i);
		tok = child->getToken(child);
		tok_type = tok->getType(tok);
		switch (tok_type) {
			case JOIN_EXPR_TOKEN:
				scan_join_expr(qi, child, tab2_sym);
				break;
			case TABLE_REF_TOKEN:
				start_tab_idx = i;
				scan_table_ref(qi, tree, &start_tab_idx, &tab2_sym);
				i = start_tab_idx;
				break;
			case SELECT_SYMBOL:
				start_sub_idx = i;
				scan_subquery(qi, tree, 1, &start_sub_idx, &tab2_sym);
				i = start_sub_idx;
				break;
			case ON_SYMBOL:
				start_join_idx = i+1;
				scan_where_expr(qi, tree, &start_join_idx, JOIN_COND);
				i = start_join_idx;
				break;
			case USING_SYMBOL:
				{
					do {
						i++;
						child = (pANTLR3_BASE_TREE) tree->getChild(tree, i);
						tok = child->getToken(child);
						tok_type = tok->getType(tok);
					} while (tok_type != IDENTIFIER);

					tok_text = child->getText(child);
					name = (char *) tok_text->chars;
					parser_context_t *pc = &qi->pc;
					context_t *currC = get_curr_context(pc);
					char buf[MAX_NAME_LEN];
					memset(buf, 0, MAX_NAME_LEN);
					sprintf(buf, "%s.%s", sym_field(tab2_sym, name), name);
					symbol_t *lookup_sym = get_symbol_by_name_in_context_list(currC, buf, SYM_COL);
					if (!lookup_sym) {
						lookup_sym = alloc_init_symbol(pc, buf, SYM_COL);
						insert_sym_to_context(currC, lookup_sym);
					}
					cs = alloc_init_col_sym(NULL, sym_field(tab2_sym, name), name, &join_xf_list, JOIN_COND);
					inserted = insert_col_sym_to_sym_list(lookup_sym, cs);
					if (inserted != cs) {
						free_col_sym(&cs);
						cs = inserted;
					}
					memset(buf, 0, MAX_NAME_LEN);
					sprintf(buf, "%s.%s", sym_field(tab1_sym, name), name);
					lookup_sym = get_symbol_by_name_in_context_list(currC, buf, SYM_COL);
					if (!lookup_sym) {
						lookup_sym = alloc_init_symbol(pc, buf, SYM_COL);
						insert_sym_to_context(currC, lookup_sym);
					}
					cs = alloc_init_col_sym(NULL, sym_field(tab1_sym, name), name, &join_xf_list, JOIN_COND);
					inserted = insert_col_sym_to_sym_list(lookup_sym, cs);
					if (inserted != cs) {
						free_col_sym(&cs);
						cs = inserted;
					}
				}
		}
	}
}

void 
scan_table_expr(query_info_int_t *qi, pANTLR3_BASE_TREE tree, 
		ANTLR3_UINT32 start_idx, ANTLR3_UINT32 *end_idx)
{
	pANTLR3_BASE_TREE child, next;
	pANTLR3_COMMON_TOKEN tok;
	ANTLR3_UINT32 i, j, num_children, tok_type, start_tab_idx, end_tab_idx, 
								start_sub_idx, end_sub_idx, end_sub = 0, end_from_idx = 0;
	symbol_t *tab1_sym = NULL, *tab2_sym = NULL;
	num_children = tree->getChildCount(tree);

	for (i = start_idx; i < num_children; i++) {
		child = (pANTLR3_BASE_TREE) tree->getChild(tree, i);
		tok = child->getToken(child);
		tok_type = tok->getType(tok);
		switch (tok_type) {
			case TABLE_REF_TOKEN:
				start_tab_idx = i;
				scan_table_ref(qi, tree, &start_tab_idx, &tab1_sym);
				end_tab_idx = i = start_tab_idx;
				break;
			case JOIN_EXPR_TOKEN:
				scan_join_expr(qi, child, tab1_sym);
				break;

			case OPEN_PAR_SYMBOL: // correct subqueries will have to come within parantheses
			case SELECT_SYMBOL:
				start_sub_idx = i;
				scan_subquery(qi, tree, 1, &start_sub_idx, &tab1_sym);
				i = start_sub_idx;
				break;

			case WHERE_SYMBOL:
			case GROUP_SYMBOL:
			case HAVING_SYMBOL:
			case ORDER_SYMBOL:
			case CLOSE_PAR_SYMBOL:
			case LIMIT_SYMBOL:
				end_from_idx = i;
				break;
		}

		if (end_from_idx)
			break;
	}

	if (!end_from_idx)
		end_from_idx = num_children;

	*end_idx = end_from_idx;
}

void 
scan_where_expr(query_info_int_t *qi, pANTLR3_BASE_TREE tree, ANTLR3_UINT32 *cidx, 
		int expr_type)
{
	pANTLR3_BASE_TREE child;
	pANTLR3_COMMON_TOKEN tok;
	ANTLR3_UINT32 tok_type, num_children, i;
	list_t where_xf_list;
	list_init(&where_xf_list);

	num_children = tree->getChildCount(tree);
	i = *cidx;
	for (; i < num_children; i++) {
		child = (pANTLR3_BASE_TREE) tree->getChild(tree, i);
		tok = child->getToken(child);
		tok_type = tok->getType(tok);
		switch (tok_type) {
			case EXPRESSION_TOKEN:
				scan_expr_tok(qi, child, &where_xf_list, expr_type, NULL);
				break;
		}
	}
}

void 
scan_select_expr_tok(query_info_int_t *qi, pANTLR3_BASE_TREE tree)
{
	pANTLR3_BASE_TREE child;
	pANTLR3_COMMON_TOKEN tok;
	pANTLR3_STRING tok_text;
	ANTLR3_UINT32 tok_type, num_children, i;
	char *expr_alias = NULL;
	context_t *currC = NULL;
	list_t sel_xf_list;
	list_init(&sel_xf_list);

	num_children = tree->getChildCount(tree);
	assert((num_children >= 1) && (num_children <= 3));

	parser_context_t *pc = &qi->pc;
	currC = get_curr_context(pc);

	if (num_children > 1) {
		child = (pANTLR3_BASE_TREE) tree->getChild(tree, (num_children -1));
		tok_text = child->getText(child);
		tok = child->getToken(child);
		tok_type = tok->getType(tok);
		assert(tok_type == IDENTIFIER);
		expr_alias = (char *) tok_text->chars;
	}

	child = (pANTLR3_BASE_TREE) tree->getChild(tree, 0);
	tok = child->getToken(child);
	tok_type = tok->getType(tok);
	assert(tok_type == EXPRESSION_TOKEN);
	scan_expr_tok(qi, child, &sel_xf_list, PROJECT, expr_alias);
}

void 
scan_spl_query(query_info_int_t *qi, pANTLR3_BASE_TREE tree, int start_idx, int end_idx)
{
	int spl_num_children;
	pANTLR3_BASE_TREE spl_child, next;
	pANTLR3_COMMON_TOKEN spl_tok;
	pANTLR3_STRING spl_tok_text;
	ANTLR3_UINT32 spl_tok_type;
	char *spl_alias;
	int i = start_idx;
	for (; i < end_idx; i++) {
		next = (pANTLR3_BASE_TREE) tree->getChild(tree, i);
		spl_tok = next->getToken(next);
		spl_tok_type = spl_tok->getType(spl_tok);
		switch (spl_tok_type) {
			case SELECT_EXPR_TOKEN:
				{
					spl_num_children = next->getChildCount(next);
					if (spl_num_children > 1) {
						spl_child = 
							(pANTLR3_BASE_TREE) next->getChild(next, (spl_num_children - 1));
						spl_tok_text = spl_child->getText(spl_child);
						spl_tok = spl_child->getToken(spl_child);
						spl_tok_type = spl_tok->getType(spl_tok);
						assert(spl_tok_type == IDENTIFIER);
						spl_alias = (char *) spl_tok_text->chars;
					}

					spl_child = (pANTLR3_BASE_TREE) next->getChild(next, 0);
					spl_tok = spl_child->getToken(spl_child);
					spl_tok_type = spl_tok->getType(spl_tok);
					assert(spl_tok_type == EXPRESSION_TOKEN);
					scan_spl_expr_tok(qi, spl_child, PROJECT, spl_alias);
				}
				break;
		}
	}
}

void 
traverse_ast_select(void *qip, pANTLR3_BASE_TREE tree, 
		ANTLR3_UINT32 *idx, int from_subquery)
{
	pANTLR3_BASE_TREE child, next, next2, node;
	pANTLR3_COMMON_TOKEN tok;
	ANTLR3_UINT32 tok_type, num_children, i, j, nidx = 0, start_idx, 
								start_from_idx = 0, end_from_idx = 0;
	int expr_type = -1, end = 0;

	context_t *parentC = NULL, *newC = NULL;
	query_info_int_t *qi = (query_info_int_t *) qip;
	parser_context_t *pc = &qi->pc;
	parentC = get_curr_context(pc);
	newC = alloc_init_context(pc);
	set_context_parent(newC, parentC);

	if (!from_subquery) {
		child = (pANTLR3_BASE_TREE) tree->getChild(tree, *idx);
		num_children = child->getChildCount(child);
		node = child;
		start_idx = 0;
	} else {
		num_children = tree->getChildCount(tree);
		node = tree;
		start_idx = *idx;
	}

	i = start_idx;
	/* 
	 * first resolve from (including any joins and subqueries in the from/join clause)
	 */
	for (; i < num_children; i++) {
		next = (pANTLR3_BASE_TREE) node->getChild(node, i);
		tok = next->getToken(next);
		tok_type = tok->getType(tok);
		switch (tok_type) {
			case FROM_SYMBOL:
				{
					next2 = (pANTLR3_BASE_TREE) node->getChild(node, i+1);
					tok = next2->getToken(next2);
					tok_type = tok->getType(tok);
					if (tok_type == DUAL_SYMBOL) {
						break;
					}
				}
				start_from_idx = i;
				scan_table_expr(qi, node, start_from_idx, &end_from_idx);
				i = end_from_idx;
				break;

				/* 
				 * this case occurs only for special types of query constructs
				 */
			case WHERE_SYMBOL:
			case GROUP_SYMBOL:
			case HAVING_SYMBOL:
			case JOIN_EXPR_TOKEN:
			case ON_SYMBOL:
			case USING_SYMBOL:
			case ORDER_SYMBOL:
			case CLOSE_PAR_SYMBOL:
			case LIMIT_SYMBOL:
				end_from_idx = i;
		}

		if (end_from_idx)
			break;
	}

	if (!end_from_idx)
		end_from_idx = num_children;

	// handle special query construct, where there is no from
	if (!start_from_idx) {
		i = start_idx;
		scan_spl_query(qi, node, i, end_from_idx);
		*idx = end_from_idx;
		return;
	}

	i = start_idx;
	/* 
	 * then resolve projection
	 */
	list_t sel_col_xf_list;
	list_init(&sel_col_xf_list);
	for (; i < start_from_idx; i++) {
		next = (pANTLR3_BASE_TREE) node->getChild(node, i);
		tok = next->getToken(next);
		tok_type = tok->getType(tok);
		switch (tok_type) {
			case SELECT_EXPR_TOKEN:
				scan_select_expr_tok(qi, next);
				break;
			case COLUMN_REF_TOKEN:
				scan_col_ref(qi, next, &sel_col_xf_list, PROJECT, NULL);
				break;
			case MULT_OPERATOR:
				{
					// add to db all cols corr. to tables that showed up in from expr above
					char *str = (char *) "*";
					col_sym_t *cs = NULL, *inserted = NULL;
					char *dbname;
					list_t sel_mul_xf_list;
					list_init(&sel_mul_xf_list);
					parser_context_t *pc = &qi->pc;
					context_t *currC = get_curr_context(pc);
					symbol_t *sym = get_symbol_by_name_in_context_list(currC, str, SYM_COL);
					if (!sym) {
						sym = alloc_init_symbol(pc, str, SYM_COL);
						insert_sym_to_context(currC, sym);
					}

					list_t *sym_head = &currC->symbol_list[SYM_TAB];
					symbol_t *sym2;
					list_for_each_entry(sym2, sym_head, symbol_listp) {
						dbname = sym_field(sym2, db_tname);
						if (dbname) {
							cs = alloc_init_col_sym(NULL, dbname, str, &sel_mul_xf_list, PROJECT);
							inserted = insert_col_sym_to_sym_list(sym, cs);
							if (inserted != cs) {
								free_col_sym(&cs);
								cs = inserted;
							}
						}
					}
				}
				break;
		}
	}

	nidx = 0;
	/* 
	 * finally resolve filter conditions - where, group by, having
	 */
	for (i = end_from_idx; i < num_children; i++) {
		next = (pANTLR3_BASE_TREE) node->getChild(node, i);
		tok = next->getToken(next);
		tok_type = tok->getType(tok);
		switch (tok_type) {
			case WHERE_SYMBOL: expr_type = FILTER;
				break;
			case HAVING_SYMBOL: expr_type = HAVING;
				break;
			case GROUP_SYMBOL: expr_type = GROUP;
				break;
			case ORDER_SYMBOL: expr_type = ORDER;
				break;
			case AS_SYMBOL:
			case JOIN_EXPR_TOKEN:
			case JOIN_SYMBOL:
			case ON_SYMBOL:
			case CLOSE_PAR_SYMBOL:
			case LIMIT_SYMBOL:
				end = 1;
				break;
		}

		if (end)
			break;
		
		if (expr_type >= FILTER && expr_type <= ORDER)
			scan_where_expr(qi, next, &nidx, expr_type);

		expr_type = -1;
	}

	// update position upto which subtree parsed in caller
	*idx = i;
}

void
resolve_symbols(void *qip)
{
	query_info_int_t *qi = (query_info_int_t *) qip;
	parser_context_t *pc = &qi->pc;
	resolve_col_sym_db_info(pc);
}

void 
do_traverse_ast_resolve(void *qi, void *tree_p)
{
	pANTLR3_BASE_TREE tree = (pANTLR3_BASE_TREE) tree_p;
	pANTLR3_BASE_TREE child;
	pANTLR3_COMMON_TOKEN tok;
	ANTLR3_UINT32 num_children, tok_type, i, sidx = 0;
	
	num_children = tree->getChildCount(tree);
	for (i = 0; i < num_children; i++) {
		child = (pANTLR3_BASE_TREE) tree->getChild(tree, i);
		tok = child->getToken(child);
		tok_type = tok->getType(tok);
		switch (tok_type) {
			case SELECT_SYMBOL:
				traverse_ast_select(qi, tree, &sidx, 0);
				resolve_symbols(qi);
				break;
		}
	}
}

// == reconstruct query string from AST ==
std::string
get_query_from_ast_int(std::string result, pANTLR3_BASE_TREE tree)
{
	pANTLR3_BASE_TREE child;
	pANTLR3_COMMON_TOKEN tok;
	pANTLR3_STRING tok_text;
	ANTLR3_UINT32 num_children, tok_type, i, sidx = 0, child_num_children;
	char *tok_str;
	
	num_children = tree->getChildCount(tree);
	for (i = 0; i < num_children; i++) {
		child = (pANTLR3_BASE_TREE) tree->getChild(tree, i);
		tok = child->getToken(child);
		tok_type = tok->getType(tok);
		switch(tok_type) {
			case COMMA_SYMBOL:
			case COALESCE_SYMBOL:
			case GROUP_CONCAT_SYMBOL:
			case SEPARATOR_SYMBOL:
			case SINGLE_QUOTED_TEXT:
			case OPEN_PAR_SYMBOL:
			case CLOSE_PAR_SYMBOL:
			case SELECT_SYMBOL:
			case FROM_SYMBOL:
			case LEFT_SYMBOL:
			case RIGHT_SYMBOL:
			case JOIN_SYMBOL:
			case USING_SYMBOL:
			case ON_SYMBOL:
			case WHERE_SYMBOL:
			case GROUP_SYMBOL:
			case HAVING_SYMBOL:
			case ORDER_SYMBOL:
			case LIMIT_SYMBOL:
			case AS_SYMBOL:
			case EQUAL_OPERATOR:
			case NOT_EQUAL_OPERATOR:
			case GREATER_THAN_OPERATOR:
			case GREATER_OR_EQUAL_OPERATOR:
			case LESS_THAN_OPERATOR:
			case LESS_OR_EQUAL_OPERATOR:
			case BITWISE_AND_OPERATOR:
			case BITWISE_OR_OPERATOR:
			case BITWISE_XOR_OPERATOR:
			case PLUS_OPERATOR:
			case MINUS_OPERATOR:
			case MULT_OPERATOR:
			case DIV_OPERATOR:
			case MOD_OPERATOR:
			case IS_SYMBOL:
			case LIKE_SYMBOL:
			case AND_SYMBOL:
			case OR_SYMBOL:
			case NOT_SYMBOL:
				tok_text = child->getText(child);
				tok_str = (char *) tok_text->chars;
				result += tok_str + std::string(" ");

			case TABLE_REF_TOKEN:
			case COLUMN_REF_TOKEN:
				{
					pANTLR3_BASE_TREE child_child;
					pANTLR3_STRING child_tok_text;
					ANTLR3_UINT32 child_num_children, child_tok_type, child_i;
					char *child_tok_str;
					child_num_children = child->getChildCount(child);
					for (child_i = 0; child_i < child_num_children; child_i++) {
						child_child = (pANTLR3_BASE_TREE) child->getChild(child, child_i);
						child_tok_text = child_child->getText(child_child);
						child_tok_str = (char *) child_tok_text->chars;
						result += child_tok_str;
					}
					result += std::string(" ");
				}

			default:
				result = get_query_from_ast_int(result, child);
		}
	}

	return result;
}

