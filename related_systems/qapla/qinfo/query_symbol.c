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

#include "query_symbol.h"

void
init_symbol(symbol_t *sym)
{
	memset(sym, 0, sizeof(symbol_t));
	list_init(&sym->col_sym_list);
	list_init(&sym->symbol_listp);
}

void
set_symbol(symbol_t *sym, char *name, int id, int type)
{
	set_sym_field(sym, type, type);
	set_sym_field(sym, id, id);
	set_sym_str_field(sym, name, name);
	set_sym_field(sym, db_tid, -1);
	set_sym_field(sym, db_cid, -1);
	set_sym_field(sym, db_tname, NULL);
	set_sym_field(sym, db_cname, NULL);
}

symbol_t *
dup_symbol(symbol_t *src)
{
	symbol_t *sym = (symbol_t *) malloc(sizeof(symbol_t));
	init_symbol(sym);

	set_sym_field_from_src(sym, type, src);
	set_sym_field_from_src(sym, id, src);
	set_sym_field_from_src(sym, db_tid, src);
	set_sym_field_from_src(sym, db_cid, src);
	set_sym_str_field_from_src(sym, name, src);
	set_sym_str_field_from_src(sym, db_tname, src);
	set_sym_str_field_from_src(sym, db_cname, src);

	col_sym_t *cs, *new_cs;
	list_t *cs_head = &src->col_sym_list;
	list_for_each_entry(cs, cs_head, col_sym_listp) {
		new_cs = dup_col_sym(cs);
		set_sym_field_from_src(new_cs, type, cs);
		list_insert(&sym->col_sym_list, &new_cs->col_sym_listp);
	}

	return sym;
}

void 
cleanup_symbol(symbol_t *sym)
{
	cleanup_col_sym_list(&sym->col_sym_list);
	free_sym_str_field(sym, name);
	free_sym_str_field(sym, db_tname);
	free_sym_str_field(sym, db_cname);
	init_symbol(sym);
}

void
free_symbol(symbol_t **sym_pp)
{
	if (!sym_pp)
		return;

	symbol_t *sym_p = *sym_pp;
	if (!sym_p)
		return;

	cleanup_symbol(sym_p);
	free(sym_p);
	*sym_pp = NULL;
}

void
cleanup_symbol_list(list_t *sym_list)
{
	list_t *sym_it, *next_sym_it, *sym_head;
	symbol_t *sym;

	sym_head = sym_list;
	sym_it = sym_head->next;
	while (sym_it != sym_head) {
		sym = list_entry(sym_it, symbol_t, symbol_listp);
		next_sym_it = sym_it->next;
		list_remove(sym_it);
#if 1
		free_symbol(&sym);
#endif
		sym_it = next_sym_it;
	}
	list_init(sym_list);
}

void 
print_symbol(symbol_t *sym, FILE *f)
{
	if (!f)
		return;

	if (sym_field(sym, name))
		fprintf(f, "<%d,%s> => ", sym_field(sym, id), sym_field(sym, name));
	else
		fprintf(f, "<%d,_> => ", sym_field(sym, id));

	print_col_sym_list(&sym->col_sym_list, f);
}

void
print_symbol_list(list_t *sym_list, FILE *f)
{
	symbol_t *sym;
	list_t *sym_head = sym_list;
	list_for_each_entry(sym, sym_head, symbol_listp) {
		print_symbol(sym, f);
	}
}

col_sym_t * 
insert_col_sym_to_sym_list(symbol_t *sym, col_sym_t *cs)
{
	int insert = 1, notinsert = 0, match = 0;
	list_t *cs_it, *cs_head;
	cs_head = &sym->col_sym_list;
	cs_it = sym->col_sym_list.next;
	col_sym_t *exists = NULL;
	col_sym_t *inserted = cs;

	while (cs_it != cs_head) {
		exists = list_entry(cs_it, col_sym_t, col_sym_listp);
		cs_it = cs_it->next;
		if ((exists->s.db_cname && !cs->s.db_cname) || 
				(!exists->s.db_cname && cs->s.db_cname))
			continue;
		if ((exists->s.db_tname && !cs->s.db_tname) || 
				(!exists->s.db_tname && cs->s.db_tname))
			continue;
		if (exists->s.db_cname && cs->s.db_cname && 
				exists->s.db_tname && cs->s.db_tname) {
			if (((strlen(exists->s.db_cname) == strlen(cs->s.db_cname)) && 
						strcmp(exists->s.db_cname, cs->s.db_cname) == 0) && 
					((strlen(exists->s.db_tname) == strlen(cs->s.db_tname)) && 
					 strcmp(exists->s.db_tname, cs->s.db_tname) == 0)) {
				insert = 0;
				//break;
			}
		} else if (exists->s.db_cname && cs->s.db_cname) {
			if ((strlen(exists->s.db_cname) == strlen(cs->s.db_cname)) && 
					strcmp(exists->s.db_cname, cs->s.db_cname) == 0) {
				insert = 0;
				//break;
			}
		} else if (exists->s.db_tname && cs->s.db_tname) {
			if ((strlen(exists->s.db_tname) == strlen(cs->s.db_tname)) &&
					strcmp(exists->s.db_tname, cs->s.db_tname) == 0) {
				insert = 0;
				//break;
			}
		}

		if (insert == 0) {
			if (exists->s.name && cs->s.name && 
					(strlen(exists->s.name) == strlen(cs->s.name)) &&
					strcmp(exists->s.name, cs->s.name) == 0) {
				inserted = exists;
				break;
			} else if (!exists->s.name && !cs->s.name) {
				inserted = exists;
				break;
			} else {
				insert = 1;
			}
		}
	}

	if (insert) {
		list_insert(&sym->col_sym_list, &cs->col_sym_listp);
		inserted = cs;
	}

	return inserted;
}

int
check_only_cid(col_sym_t *to_insert, col_sym_t *exists)
{
	if (!to_insert || !exists)
		return 0;

	int i_tid = sym_field(to_insert, db_tid);
	int i_cid = sym_field(to_insert, db_cid);
	int e_tid = sym_field(exists, db_tid);
	int e_cid = sym_field(exists, db_cid);

	return (i_tid == e_tid && i_cid == e_cid);
}

col_sym_t *
insert_col_sym_to_sym_list_fn(symbol_t *sym, col_sym_t *cs, int check_type)
{
	int insert = 1, notinsert = 0, match = 0;
	col_sym_t *exists = NULL;
	col_sym_t *inserted = cs;
	int e_ret = 0;
	check_fn fn = NULL;

	switch (check_type) {
		case CHECK_ONLY_CID: fn = &check_only_cid;
			break;
	}

	list_for_each_entry(exists, &sym->col_sym_list, col_sym_listp) {
		if (fn)
			e_ret = fn(cs, exists);
		if (!e_ret)
			continue;

		insert = 0;
		inserted = exists;
		break;
	}

	if (insert) {
		list_insert(&sym->col_sym_list, &cs->col_sym_listp);
		inserted = cs;
	}

	return inserted;
}

col_sym_t *
get_col_sym_by_expr_type_in_sym(symbol_t *sym, int expr_type, list_t **next)
{
	if (!next)
		return NULL;

	list_t *cs_it, *cs_head;
	cs_head = &sym->col_sym_list;

	if (!(*next))
		cs_it = sym->col_sym_list.next;
	else
		cs_it = *next;

	while (cs_it != cs_head) {
		col_sym_t *cs = list_entry(cs_it, col_sym_t, col_sym_listp);
		if (!cs)
			break;

		cs_it = cs_it->next;
		if (expr_type < 0 || (sym_field(cs, type) == expr_type)) {
			*next = cs_it;
			return cs;
		}
	}

	return NULL;
}

