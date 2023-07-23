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

#include "common/db.h"
#include "common/sym.h"
#include "sym_xform.h"
#include "col_sym.h"

const char *expr_type_name[] = {"P", "T", "F", "J", "G", "H", "O"};

static char *
get_expr_type_name(int expr_type)
{
	char *expr_name;
	int expr_bit_type;
	if (expr_type & PROJECT)
		expr_name = (char *) "P";
	else if (expr_type & TABLE)
		expr_name = (char *) "T";
	else if (expr_type & FILTER)
		expr_name = (char *) "F";
	else if (expr_type & JOIN_COND)
		expr_name = (char *) "J";
	else if (expr_type & GROUP)
		expr_name = (char *) "G";
	else if (expr_type & HAVING)
		expr_name = (char *) "H";
	else if (expr_type & ORDER)
		expr_name = (char *) "O";
	else
		expr_name = (char *) "NIL";
	return expr_name;
}

void 
generate_col_sym_alias(char *abuf, char *tname, char *cname, list_t *xf_list)
{
	char buf[MAX_NAME_LEN];
	memset(buf, 0, MAX_NAME_LEN);
	char *ptr = buf;
	if (tname) {
		sprintf(ptr, "%s", tname);
		ptr += strlen(tname);
	}
	if (tname && cname) {
		sprintf(ptr, ".");
		ptr += 1;
	}
	if (cname) {
		sprintf(ptr, "%s", cname);
		ptr += strlen(cname);
	}
	get_xform_list(abuf, NULL, xf_list, buf);
}

void 
add_rev_xf_list_to_col_sym_at_head(col_sym_t *cs, list_t *xf_list)
{
	list_t *xf_it, *xf_prev;
	xf_it = xf_list->prev;
	while (xf_it != xf_list) {
		xf_prev = xf_it->prev;
		list_remove(xf_it);
		list_insert(&cs->xform_list, xf_it);
		xf_it = xf_prev;
	}
}

void
init_col_sym(col_sym_t *cs)
{
	memset(cs, 0, sizeof(col_sym_t));
	list_init(&cs->col_sym_listp);
	list_init(&cs->xform_list);
}

void
set_col_sym_tid_cid(col_sym_t *cs, int tid, int cid)
{
	if (!cs)
		return;

	set_sym_field(cs, db_tid, tid);
	set_sym_field(cs, db_cid, cid);
}

col_sym_t *
alloc_init_col_sym(char *alias, char *tname, char *cname, list_t *xf_list, int expr_type)
{
	col_sym_t *cs = (col_sym_t *) malloc(sizeof(col_sym_t));
	init_col_sym(cs);

	set_sym_str_field(cs, db_tname, tname);
	set_sym_str_field(cs, db_cname, cname);
	set_sym_field(cs, type, expr_type);

	if (xf_list)
		add_rev_xf_list_to_col_sym_at_head(cs, xf_list);

	if (!alias) {
		char buf[MAX_NAME_LEN];
		memset(buf, 0, MAX_NAME_LEN);
		generate_col_sym_alias(buf, tname, cname, &cs->xform_list);
		set_sym_str_field(cs, name, buf);
	} else {
		set_sym_str_field(cs, name, alias);
	}
	return cs;
}

void
init_col_sym_from_schema(col_sym_t *cs, db_t *schema, char *tname, char *cname)
{
	if (!cs)
		return;

	init_col_sym(cs);

	int tid = get_schema_table_id(schema, tname);
	int cid = get_schema_col_id_in_table_id(schema, tid, cname);
	set_sym_field(cs, db_tid, tid);
	set_sym_field(cs, db_cid, cid);
	set_sym_str_field(cs, db_tname, tname);
	set_sym_str_field(cs, db_cname, cname);
}

col_sym_t *
alloc_init_col_sym_from_schema(db_t *schema, char *tname, char *cname)
{
	col_sym_t *cs = (col_sym_t *) malloc(sizeof(col_sym_t));
	init_col_sym_from_schema(cs, schema, tname, cname);
	return cs;
}

col_sym_t *
dup_col_sym(col_sym_t *src)
{
	col_sym_t *cs = (col_sym_t *) malloc(sizeof(col_sym_t));
	init_col_sym(cs);

	set_sym_field_from_src(cs, type, src);
	set_sym_field_from_src(cs, id, src);
	set_sym_field_from_src(cs, db_tid, src);
	set_sym_field_from_src(cs, db_cid, src);
	set_sym_str_field_from_src(cs, name, src);
	set_sym_str_field_from_src(cs, db_tname, src);
	set_sym_str_field_from_src(cs, db_cname, src);

	dup_transform(&cs->xform_list, &src->xform_list);

	return cs;
}

int
compare_col_sym(col_sym_t *c1, col_sym_t *c2, int exact_match)
{
	if (!c1 && !c2)
		return 0;

	if ((c1 && !c2) || (c2 && !c1))
		return -1;

	int cid1 = sym_field(c1, db_cid);
	int cid2 = sym_field(c2, db_cid);
	list_t *xf1 = &c1->xform_list;
	list_t *xf2 = &c2->xform_list;

	if (cid1 != cid2)
		return -1;

	if (!xf1 && !xf2)
		return 0;

	if (!exact_match)
		return 0;

	int ret = cmp_transform(xf1, xf2);
	return ret;
}

int
exists_col_sym_in_list(col_sym_t *cs, list_t *cs_list, int exact_match, 
		col_sym_t **match_cs)
{
	col_sym_t *e_cs;
	list_t *cs_it = cs_list;
	int ret;

	list_for_each_entry(e_cs, cs_it, col_sym_listp) {
		ret = compare_col_sym(cs, e_cs, exact_match);
		if (ret == 0) {
			*match_cs = e_cs;
			return ret;
		}
	}

	return -1;
}

void 
cleanup_col_sym(col_sym_t *cs)
{
	if (!cs)
		return;

	free_sym_str_field(cs, name);
	free_sym_str_field(cs, db_tname);
	free_sym_str_field(cs, db_cname);
	free_transform_list(&cs->xform_list);
	init_col_sym(cs);
}

void
free_col_sym(col_sym_t **cs_pp)
{
	if (!cs_pp)
		return;

	col_sym_t *cs_p = *cs_pp;
	cleanup_col_sym(cs_p);
	if (cs_p)
		free(cs_p);
	*cs_pp = NULL;
}

void
cleanup_col_sym_list(list_t *cs_list)
{
	list_t *cs_it, *next_cs_it, *cs_head;
	col_sym_t *cs;

	cs_head = cs_list;
	cs_it = cs_head->next;
	while (cs_it != cs_head) {
		cs = list_entry(cs_it, col_sym_t, col_sym_listp);
		next_cs_it = cs_it->next;
		list_remove(cs_it);
		free_col_sym(&cs);
		cs_it = next_cs_it;
	}
	list_init(cs_list);
}

void 
print_col_sym(col_sym_t *cs, FILE *f)
{
	if (!f)
		return;

	char buf[MAX_NAME_LEN], buf2[MAX_NAME_LEN];
	memset(buf, 0, MAX_NAME_LEN);
	memset(buf2, 0, MAX_NAME_LEN);
	if (sym_field(cs, db_tname) && sym_field(cs, db_cname))
		sprintf(buf, "%s.%s (%d.%d)", sym_field(cs, db_tname), sym_field(cs, db_cname), 
				sym_field(cs, db_tid), sym_field(cs, db_cid));
	else if (sym_field(cs, db_tname) && !sym_field(cs, db_cname))
		sprintf(buf, "%s._ (%d._)", sym_field(cs, db_tname), sym_field(cs, db_tid));
	else if (sym_field(cs, db_cname) && !sym_field(cs, db_tname))
		sprintf(buf, "_.%s (_.%d)", sym_field(cs, db_cname), sym_field(cs, db_cid));
	else
		sprintf(buf, "_._"); // this should never happen

	char expr_name[32];
	memset(expr_name, 0, 32);
	sprintf(expr_name, "%s:", get_expr_type_name(sym_field(cs, type)));
	get_xform_list(buf2, expr_name, &cs->xform_list, buf);
	fprintf(f, "%s", buf2);
}

void
print_col_sym_list(list_t *cs_list, FILE *f)
{
	list_t *cs_head = cs_list;
	list_t *cs_it = cs_head->next;
	if (cs_it != cs_head)
		fprintf(f, "[");
	else {
		fprintf(f, "NULL\n");
		return;
	}

	while (cs_it->next != cs_head) {
		col_sym_t *cs = list_entry(cs_it, col_sym_t, col_sym_listp);
		print_col_sym(cs, f);
		fprintf(f, ", ");
		cs_it = cs_it->next;
	}

	col_sym_t *cs = list_entry(cs_it, col_sym_t, col_sym_listp);
	print_col_sym(cs, f);
	fprintf(f, "]\n");
}

