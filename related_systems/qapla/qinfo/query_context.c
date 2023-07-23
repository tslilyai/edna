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

#include "query_context.h"
#include "query_symbol.h"

void
init_parser_context(parser_context_t *pc)
{
	if (!pc)
		return;

	memset(pc, 0, sizeof(parser_context_t));
	list_init(&pc->context_stack.list);
	pc->context_stack.tos = &pc->context_stack.list;
}

void
cleanup_parser_context(parser_context_t *pc)
{
	if (!pc)
		return;

	list_t *context_it, *next_context_it, *context_head;
	context_t *context;
	context_head = &pc->context_stack.list;
	context_it = context_head->next;
	while (context_it != context_head) {
		context = list_entry(context_it, context_t, context_listp);
		next_context_it = context_it->next;
		list_remove(context_it);
		free_context(&context);
		context_it = next_context_it;
	}

	init_parser_context(pc);
}

int
is_parser_context_empty(parser_context_t *pc)
{
	return list_empty(&pc->context_stack.list);
}

void
init_context(context_t *context)
{
	memset(context, 0, sizeof(context_t));

	int i;
	for (i = 0; i < NUM_SYM_TYPE; i++) {
		list_init(&context->symbol_list[i]);
	}
	list_init(&context->context_listp);
}

context_t *
alloc_init_context(parser_context_t *pc)
{
	context_t *context = (context_t *) malloc(sizeof(context_t));
	init_context(context);

	context->id = pc->num_context++;

	list_insert(pc->context_stack.tos, &context->context_listp);
	
	context->context_stackp = &pc->context_stack;
	context->prev_tos = pc->context_stack.tos;
	pc->context_stack.tos = &context->context_listp;
	
	return context;
}

void
cleanup_context(context_t *context)
{
	if (!context)
		return;

	list_t *sym_it, *next_sym_it, *sym_head;
	symbol_t *sym;

	int i;
	for (i = 0; i < NUM_SYM_TYPE; i++) {
		sym_head = &context->symbol_list[i];
		cleanup_symbol_list(sym_head);
	}

	init_context(context);
}

void
free_context(context_t **context)
{
	if (!context)
		return;

	context_t *c = *context;
	if (!c)
		return;

	cleanup_context(c);
	free(c);
	*context = NULL;

	return;
}

void
set_context_parent(context_t *context, context_t *parent)
{
	context->parent = parent;
	if (parent)
		parent->children[parent->child_context_idx++] = context;
}

context_t *
get_curr_context(parser_context_t *pc)
{
	context_t *currC = NULL;
	if (!is_parser_context_empty(pc))
		currC = list_entry(pc->context_stack.tos, context_t, context_listp);
	return currC;
}

context_t *
get_first_context(parser_context_t *pc)
{
	context_t *firstC = NULL;
	list_t *first = NULL;
	if (!is_parser_context_empty(pc)) {
		first = pc->context_stack.list.next;
		firstC = list_entry(first, context_t, context_listp);
	}
	return firstC;
}

// == symbol ==
symbol_t *
alloc_init_symbol(parser_context_t *pc, char *name, int type)
{
	symbol_t *sym = (symbol_t *) malloc(sizeof(symbol_t));
	init_symbol(sym);
	set_symbol(sym, name, pc->num_symbol++, type);
	return sym;
}

void 
insert_sym_to_context(context_t *context, symbol_t *sym)
{
	list_insert(&context->symbol_list[sym_field(sym, type)], &sym->symbol_listp);
}

symbol_t *
get_symbol_by_name_in_context_list(context_t *context, char *name, int list)
{
	if (!context || !name)
		return NULL;

	symbol_t *found = NULL;
	list_t *it = context->symbol_list[list].next;
	list_t *it_head = &context->symbol_list[list];
	while (it != it_head) {
		found = list_entry(it, symbol_t, symbol_listp);
		it = it->next;

		if (strlen(sym_field(found, name)) != strlen(name))
			continue;

		if (strcmp(sym_field(found, name), name) == 0)
			return found;
	}

	return NULL;
}

int
dup_context_symbol_list(context_t *context, int list, list_t *dup_list)
{
	if (!dup_list)
		return -1;

	list_t *sym_head = NULL;
	symbol_t *sym = NULL, *dup_sym = NULL;

	sym_head = &context->symbol_list[list];
	list_for_each_entry(sym, sym_head, symbol_listp) {
		dup_sym = dup_symbol(sym);
		list_insert(dup_list, &dup_sym->symbol_listp);
	}

  return 0;
}

symbol_t *
get_next_symbol_by_expr_type_in_context_list(context_t *context, int expr_type, 
		int list, list_t **next)
{
	if (!next)
		return NULL;

	list_t *sym_it = NULL, *sym_head = NULL, *cs_next = NULL;
	col_sym_t *cs = NULL, *new_cs = NULL;
	symbol_t *sym = NULL, *new_sym = NULL;
	int found = 0;

	sym_head = &context->symbol_list[list];

	if (!(*next))
		sym_it = sym_head->next;
	else
		sym_it = *next;

	while (sym_it != sym_head) {
		found = 0;
		sym = list_entry(sym_it, symbol_t, symbol_listp);
		if (!sym)
			break;

		sym_it = sym_it->next;

		cs = get_col_sym_by_expr_type_in_sym(sym, expr_type, &cs_next);
		if (cs) {
			found = 1;
			new_sym = (symbol_t *) malloc(sizeof(symbol_t));
			init_symbol(new_sym);
			set_symbol(new_sym, sym_field(sym, name), sym_field(sym, id), 
					sym_field(sym, type));
		}

		while (cs) {
			new_cs = dup_col_sym(cs);
			set_sym_field_from_src(new_cs, type, cs);
			set_sym_str_field_from_src(new_cs, name, new_sym);
			insert_col_sym_to_sym_list(new_sym, new_cs);
			cs = get_col_sym_by_expr_type_in_sym(sym, expr_type, &cs_next);
		}

		if (found) {
			*next = sym_it;
			return new_sym;
		}
	}

	return NULL;
}

int 
get_schema_tid_for_context_col_sym(context_t *context, db_t *schema, char *cname)
{
	int db_tid, db_cid;
	list_t *tab_head = &context->symbol_list[SYM_TAB];
	symbol_t *tab_sym;
	list_for_each_entry(tab_sym, tab_head, symbol_listp) {
		if (sym_field(tab_sym, db_tname)) {
			db_tid = get_schema_table_id(schema, sym_field(tab_sym, db_tname));
			if (db_tid >= 0) {
				db_cid = get_schema_col_id_in_table_id(schema, db_tid, cname);
				if (db_cid >= 0)
					return db_tid;
			}
		} else {
			list_t *cs_head = &tab_sym->col_sym_list;
			col_sym_t *cs;
			list_for_each_entry(cs, cs_head, col_sym_listp) {
				if (sym_field(cs, db_tname)) {
					db_tid = get_schema_table_id(schema, sym_field(cs, db_tname));
					if (db_tid >= 0) {
						db_cid = get_schema_col_id_in_table_id(schema, db_tid, cname);
						if (db_cid >= 0)
							return db_tid;
					}
				}
			}
		}
	}

	return -1;
}

static int
str_contains(char *str, const char *pattern)
{
	int str_len = strlen(str);
	int pattern_len = strlen(pattern);
	if (pattern_len > str_len)
		return 0;

	if (strstr(str, pattern))
		return 1;

	return 0;
}

void
resolve_star_sym_db_info(parser_context_t *pc)
{
	context_t *currC = get_curr_context(pc);
	list_t *sym_head, *cs_head, *sym_it, *next_sym_it, *cs_it, *next_cs_it;
	symbol_t *sym;
	col_sym_t *cs, *new_cs;
	sym_head = &currC->symbol_list[SYM_COL];
	sym_it = sym_head->next;
	while (sym_it != sym_head) {
		next_sym_it = sym_it->next;
		sym = list_entry(sym_it, symbol_t, symbol_listp);
		if (str_contains(sym_field(sym, name), "*")) {
			cs_head = &sym->col_sym_list;
			cs_it = cs_head->next;
			while (cs_it != cs_head) {
				next_cs_it = cs_it->next;
				cs = list_entry(cs_it, col_sym_t, col_sym_listp);
				if (str_contains(sym_field(cs, db_cname), "*")) {
					list_remove(cs_it);
					int db_tid = get_schema_table_id(pc->schema, sym_field(cs, db_tname));
					int num_col = get_schema_num_col_in_table_id(pc->schema, db_tid);
					char *db_cname;
					int i;
					list_t dup_xf_list;
					list_init(&dup_xf_list);
					for (i = 0; i < num_col; i++) {
						db_cname = get_schema_col_name_in_table_id(pc->schema, db_tid, i);
						dup_transform(&dup_xf_list, &cs->xform_list);
						new_cs = alloc_init_col_sym(sym_field(sym, name), sym_field(cs, db_tname),
								db_cname, &dup_xf_list, sym_field(cs, type));
						insert_col_sym_to_sym_list(sym, new_cs);
					}
					free_col_sym(&cs);
				}
				cs_it = next_cs_it;
			}
		}
		sym_it = next_sym_it;
	}
}

void
resolve_col_sym_db_info(parser_context_t *pc)
{
	context_t *currC = get_curr_context(pc);
	list_t *sym_head, *cs_head;
	symbol_t *sym;
	col_sym_t *cs;
	int i;

	resolve_star_sym_db_info(pc);

	for (i = 0; i < NUM_SYM_TYPE; i++) {
		sym_head = &currC->symbol_list[i];
		list_for_each_entry(sym, sym_head, symbol_listp) {
			cs_head = &sym->col_sym_list;
			list_for_each_entry(cs, cs_head, col_sym_listp) {
				int tid = sym_field(cs, db_tid);
				if (tid == DUAL_TID)
					continue;

				char *tname = sym_field(cs, db_tname);
				char *dual = (char *) DUAL_TNAME;
				if ((strlen(tname) == strlen(dual)) && strcmp(tname, dual) == 0) {
					set_sym_field(cs, db_tid, DUAL_TID);
					continue;
				}

				if (tname) {
					int tid = get_schema_table_id(pc->schema, tname);
					set_sym_field(cs, db_tid, tid);
				}

				char *cname = sym_field(cs, db_cname);
				if (cname && strcmp(cname, "*") != 0) {
					int cid = get_schema_col_id_in_table_id(pc->schema, 
							sym_field(cs, db_tid), cname);
					set_sym_field(cs, db_cid, cid);
				}
			}
		}
	}
}

/* 
 * order of resolution --
 * case I: cname != "*"
 * if tname non-null
 * 	get the symbol with tname defined in current context
 * 	if sym has db_tname
 * 		add db_tname, cname into qi.db
 * 	else
 * 		go through col_sym list of sym
 * 			if tab_cs.db_cname = cname
 * 				insert tab_cs.db_tname, cname into qi.db
 * 			if tab_cs.name = cname // derived/aliased col
 * 				cs = dup(tab_cs)
 * 				add current function to cs
 * 				insert tab_cs.db_tname, tab_cs.db_cname into qi.db
 *
 * 	if tname is null
 * 		search for a table in current context whose schema contains cname
 * 		if found table
 * 			insert table.name, cname into qi.db
 * 		else
 * 			go through children of current context
 * 				search for a symbol with sym.name=cname
 * 				if sym.db_tname non null
 * 					go through col_sym list of sym
 * 						if ctx_cs.db_cname = cname
 * 							insert ctx_cs.db_tname, ctx_cs.db_cname into qi.db
 * 						if ctx_cs.name = cname
 * 							cs = dup(ctx_cs)
 * 							add current function to cs
 * 							insert ctx_cs.db_tname, ctx_cs.db_cname into qi.db
 *
 * 				if sym.db_tname is null
 * 					continue to next child
 *
 * case II: cname == "*"
 * if tname non-null
 * 	get the symbol with tname defined in current context
 * 	if sym has db_tname
 * 		add all col of db_tname into qi.db
 * 	else
 * 		go through col_sym list of sym
 * 			if tab_cs.db_cname != "*"
 * 				insert tab_cs.db_tname, tab_cs.db_cname into qi.db
 * 			else
 * 				add all col of tab_cs.db_tname into qi.db
 *
 * else // if tname is null
 * 	go through all symbols in current context
 * 		if sym.db_tname non null // sym.db_cname always null
 * 			load all col of db_tname into qi.db
 * 		else // tab.col of outer query, or from/join sub-table alias
 * 			go through col_sym list of sym
 * 				if tab_cs.db_cname != "*"
 * 					load tab_cs.db_tname, tab_cs.db_cname into qi.db
 * 				else
 * 					add all col of tab_cs.db_tname into qi.db
 */

void 
insert_resolved_col_sym(parser_context_t *pc, symbol_t *sym, char *tname, 
		char *cname, list_t *xf_list, int expr_type)
{
	col_sym_t *cs = NULL;
	col_sym_t *inserted = NULL;
	context_t *currC = get_curr_context(pc);

	int db_tid = 0, db_cid = 0;

	if (tname) {
		/* 
		 * table symbol must already have been resolved, 
		 * unless it is an alias for a temp table in a from/join subquery
		 */

		symbol_t *tab;
		col_sym_t *tab_cs;
		tab = get_symbol_by_name_in_context_list(currC, tname, SYM_TAB);
		/*
		 * for correlated sub-queries, a column from outer table may be 
		 * referred to in the inner sub-query.
		 */
		if (!tab) {
			tab = get_symbol_by_name_in_context_list(currC->parent, tname, SYM_TAB);
		}
		if (sym_field(tab, db_tname)) {
			cs = alloc_init_col_sym(sym_field(sym, name), sym_field(tab, db_tname), cname,
					xf_list, expr_type);
			inserted = insert_col_sym_to_sym_list(sym, cs);
			if (inserted != cs) {
				free_col_sym(&cs);
				cs = inserted;
			}
		} else {
			list_t *cs_head = &tab->col_sym_list;
			list_for_each_entry(tab_cs, cs_head, col_sym_listp) {
				char *tab_db_cname = sym_field(tab_cs, db_cname);
				char *tab_cname = sym_field(tab_cs, name);
				if (strcmp(tab_db_cname, cname) == 0) {
					cs = alloc_init_col_sym(sym_field(sym, name), sym_field(tab_cs, db_tname),
							cname, xf_list, expr_type);
					inserted = insert_col_sym_to_sym_list(sym, cs);
					if (inserted != cs) {
						free_col_sym(&cs);
						cs = inserted;
					}
				} else if (tab_cname && strcmp(tab_cname, cname) == 0) {
					cs = dup_col_sym(tab_cs);
					set_sym_field(cs, type, expr_type);
					if (sym_field(sym, name)) {
						free_sym_str_field(cs, name);
						set_sym_str_field_from_src(cs, name, sym);
					}
					add_rev_xf_list_to_col_sym_at_head(cs, xf_list);
					inserted = insert_col_sym_to_sym_list(sym, cs);
					if (inserted != cs) {
						free_col_sym(&cs);
						cs = inserted;
					}
				} else if (strcmp(tab_db_cname, "*") == 0) {
					db_cid = get_schema_col_id_in_table(pc->schema, 
							sym_field(tab_cs, db_tname), cname);
					if (db_cid >= 0) {
						cs = alloc_init_col_sym(sym_field(sym, name), sym_field(tab_cs, db_tname), 
								cname, xf_list, expr_type);
						inserted = insert_col_sym_to_sym_list(sym, cs);
						if (inserted != cs) {
							free_col_sym(&cs);
							cs = inserted;
						}
					}
				}
			}

			if (!cs)
				assert(0);
		}
	} else {
		int ctxdb_tid = get_schema_tid_for_context_col_sym(currC, pc->schema, cname);
		char *ctxdb_tname, *ctxdb_cname;
		if (ctxdb_tid >= 0) {
			ctxdb_tname = get_schema_table_name(pc->schema, ctxdb_tid);
			ctxdb_cname = cname;
			cs = alloc_init_col_sym(sym_field(sym, name), ctxdb_tname, ctxdb_cname,
					xf_list, expr_type);
			inserted = insert_col_sym_to_sym_list(sym, cs);
			if (inserted != cs) {
				free_col_sym(&cs);
				cs = inserted;
			}
		} else {
		// XXX: as a heuristic, currently only look through first-level children, 
		// must convert this to a full recursion through tree of children in case
		// we get queries with arbitrary level of nesting
			int cit = 0;
			for (cit = 0; cit < currC->child_context_idx; cit++) {
				symbol_t *ctx_sym = 
					get_symbol_by_name_in_context_list(currC->children[cit], cname, SYM_COL);
				if (ctx_sym && ctx_sym->s.db_tname) {
					ctxdb_tid = get_schema_table_id(pc->schema, ctx_sym->s.db_tname);
					ctxdb_tname = ctx_sym->s.db_tname;
					ctxdb_cname = cname;
					assert(0); // we shouldn't reach here in a single nested subquery 
				} else if (ctx_sym) {
					col_sym_t *ctx_cs;
					list_t *ctx_cs_head = &ctx_sym->col_sym_list;
					list_for_each_entry(ctx_cs, ctx_cs_head, col_sym_listp) {
						char *ctx_cs_db_tname = sym_field(ctx_cs, db_tname);
						if (ctx_cs_db_tname) {
							ctxdb_tid = get_schema_table_id(pc->schema, ctx_cs_db_tname);
							ctxdb_tname = ctx_cs_db_tname;
							ctxdb_cname = sym_field(ctx_cs, db_cname);
						}
					}
				}
				if (ctxdb_tid >= 0)
					break;
			}
			if (ctxdb_tid >= 0) {
				cs = alloc_init_col_sym(sym_field(sym, name), ctxdb_tname, ctxdb_cname,
						xf_list, expr_type);
				inserted = insert_col_sym_to_sym_list(sym, cs);
				if (inserted != cs) {
					free_col_sym(&cs);
					cs = inserted;
				}
			}
		}
		// XXX: else what?
	}

	if (!cs)
		assert(0);
}

void 
insert_resolved_star_sym(parser_context_t *pc, symbol_t *sym, char *tname, 
		char *cname, list_t *xf_list, int expr_type)
{
	col_sym_t *cs = NULL;
	col_sym_t *inserted = NULL;
	int db_tid = 0, db_cid = 0;
	context_t *currC = get_curr_context(pc);

	if (tname) {
		symbol_t *tab = get_symbol_by_name_in_context_list(currC, tname, SYM_TAB);
		char *db_tname = sym_field(tab, db_tname);
		if (db_tname) {
			cs = alloc_init_col_sym(sym_field(sym, name), db_tname, cname, xf_list, expr_type);
			inserted = insert_col_sym_to_sym_list(sym, cs);
			if (inserted != cs) {
				free_col_sym(&cs);
				cs = inserted;
			}
		} else {
			col_sym_t *tab_cs;
			list_t *cs_head = &tab->col_sym_list;
			list_for_each_entry(tab_cs, cs_head, col_sym_listp) {
				char *db_cname = sym_field(tab_cs, db_cname);
				if (strcmp(db_cname, cname) != 0) {
					cs = alloc_init_col_sym(sym_field(tab_cs, name), sym_field(tab_cs, db_tname),
							db_cname, xf_list, expr_type);
					inserted = insert_col_sym_to_sym_list(sym, cs);
					if (inserted != cs) {
						free_col_sym(&cs);
						cs = inserted;
					}
				} else {
					cs = dup_col_sym(tab_cs);
					set_sym_field(cs, type, expr_type);
					add_rev_xf_list_to_col_sym_at_head(cs, xf_list);
					inserted = insert_col_sym_to_sym_list(sym, cs);
					if (inserted != cs) {
						free_col_sym(&cs);
						cs = inserted;
					}
				}
			}
		}
	} else {
		symbol_t *tab;
		list_t *tab_head = &currC->symbol_list[SYM_TAB];
		list_for_each_entry(tab, tab_head, symbol_listp) {
			char *db_tname = sym_field(tab, db_tname);
			if (db_tname) {
				cs = alloc_init_col_sym(NULL, db_tname, cname, xf_list, expr_type);
				inserted = insert_col_sym_to_sym_list(sym, cs);
				if (inserted != cs) {
					free_col_sym(&cs);
					cs = inserted;
				}
			} else {
				col_sym_t *tab_cs;
				list_t *cs_head = &tab->col_sym_list;
				list_for_each_entry(tab_cs, cs_head, col_sym_listp) {
					char *db_cname = sym_field(tab_cs, db_cname);
					if (strcmp(db_cname, cname) != 0) {
						cs = alloc_init_col_sym(sym_field(tab_cs, name), sym_field(tab_cs, db_tname),
								cname, xf_list, expr_type);
						inserted = insert_col_sym_to_sym_list(sym, cs);
						if (inserted != cs) {
							free_col_sym(&cs);
							cs = inserted;
						}
					} else {
						cs = dup_col_sym(tab_cs);
						set_sym_field(cs, type, expr_type);
						add_rev_xf_list_to_col_sym_at_head(cs, xf_list);
						inserted = insert_col_sym_to_sym_list(sym, cs);
						if (inserted != cs) {
							free_col_sym(&cs);
							cs = inserted;
						}
					}
				}
			}
		}
	}

	if (!cs)
		assert(0);
}

void 
insert_resolved_col_sym_to_sym_list(parser_context_t *pc, symbol_t *sym, 
		char *tname, char *cname, list_t *xf_list, int expr_type)
{
	if (strcmp(cname, "*") != 0) {
		insert_resolved_col_sym(pc, sym, tname, cname, xf_list, expr_type);
	} else {
		insert_resolved_star_sym(pc, sym, tname, cname, xf_list, expr_type);
	}
}

