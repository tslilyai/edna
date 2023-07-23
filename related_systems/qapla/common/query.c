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

#include "utils/strops.h"
#include "utils/format.h"
#include "utils/list.h"

#include "config.h"
#include "query.h"
#include "col_sym.h"

#include "mysqlparser/mysql-parser.h"
#include "mysqlparser/mysql-scanner.h"

#include <antlr3.h>

#ifdef MAX_BUF_LEN
#undef MAX_BUF_LEN
#endif
#define MAX_BUF_LEN 1024

extern void *qapla_get_parameters(char *text, size_t length, list_t *plist);
extern void qapla_reset_scanner(void *scan);

extern int murmur_hash(void *key, int key_len, int modulo);

typedef struct qstr_int {
	char *orig_q;
	char *rewritten_q;
	int orig_qlen;
	int rewritten_qlen;
	int num_lines;
	int max_lines;
	int *line_off;
	int num_qpos;
	int8_t pol_eval_method;
	list_t qpos_list;
	list_t expected_col_list;
	list_t unexpected_col_list;
	list_t qsplit_list;
} qstr_int_t;

void
free_qpos(qpos_t **qp)
{
	if (!qp)
		return;

	qpos_t *qpp = *qp;
	if (!qpp)
		return;

	free(qpp);
	*qp = NULL;
}

void
cleanup_qpos_list(list_t *qpos_list)
{
	assert(qpos_list);

	list_t *qp_head = qpos_list;
	list_t *qp_it = qp_head->next;
	list_t *qp_next;
	qpos_t *qp;
	while (qp_it != qp_head) {
		qp_next = qp_it->next;
		qp = list_entry(qp_it, qpos_t, qpos_listp);
		list_remove(qp_it);
		free_qpos(&qp);
		qp_it = qp_next;
	}

	list_init(qpos_list);
}

qelem_t *
alloc_init_qelem(int elem_type)
{
	qelem_t *e = (qelem_t *) malloc(sizeof(qelem_t));
	memset(e, 0, sizeof(qelem_t));
	e->elem_type = elem_type;
	e->max_lines = 10;
	e->num_lines = 1;
	e->line_off = (int *) malloc(sizeof(int) * e->max_lines);
	int i;
	for (i = 0; i < e->max_lines; i++)
		e->line_off[i] = 0;
	list_init(&e->qsplit_listp);
	return e;
}

qelem_t *
clone_qelem(qelem_t *e_src)
{
	qelem_t *e_dst = NULL;
	e_dst = (qelem_t *) malloc(sizeof(qelem_t));
	memset(e_dst, 0, sizeof(qelem_t));
	list_init(&e_dst->qsplit_listp);
	qelem_set_strtype_str(e_dst, QE_STR_ORIG, e_src->orig_str, e_src->orig_len);
	qelem_set_strtype_str(e_dst, QE_STR_NEW, e_src->new_str, e_src->new_len);
	qelem_set_strtype_str(e_dst, QE_STR_ALIAS, e_src->alias_str, e_src->alias_len);
	e_dst->elem_type = e_src->elem_type;
	e_dst->max_lines = e_src->max_lines;
	e_dst->num_lines = e_src->num_lines;
	e_dst->line_off = (int *) malloc(sizeof(int) * e_dst->max_lines);
	int i;
	for (i = 0; i < e_src->num_lines; i++)
		e_dst->line_off[i] = e_src->line_off[i];
	//memcpy((char *) e_dst->line_off, (char *) e_src->line_off, (sizeof(int) * e_dst->max_lines));

#if 0
	FILE *f = fopen("/tmp/dbgphplog.txt", "a");
	if (f) {
		fprintf(f, "src[%p %p %p] %p (%d) %s, (%d) %s, (%d) %s\n", &e_src->qsplit_listp,
				e_src->qsplit_listp.prev, e_src->qsplit_listp.next, e_src->line_off,
				e_src->orig_len, e_src->orig_str, e_src->new_len, e_src->new_str, 
				e_src->alias_len, e_src->alias_str);
		fprintf(f, "dst[%p %p %p] %p (%d) %s, (%d) %s, (%d) %s\n", &e_dst->qsplit_listp,
				e_dst->qsplit_listp.prev, e_dst->qsplit_listp.next, e_dst->line_off,
				e_dst->orig_len, e_dst->orig_str, e_dst->new_len, e_dst->new_str,
				e_dst->alias_len, e_dst->alias_str);
		fclose(f);
	}
#endif
	return e_dst;
}

void
clone_qelem_list(list_t *src_list, list_t *dst_list)
{
	qelem_t *e_src = NULL, *e_dst = NULL;
	list_for_each_entry(e_src, src_list, qsplit_listp) {
		e_dst = clone_qelem(e_src);
		list_insert(dst_list, &e_dst->qsplit_listp);
	}
}

void
free_qelem(qelem_t **e)
{
	if (!e)
		return;

	qelem_t *ep = *e;
	if (!ep)
		return;

#if 0
	FILE *f = fopen("/tmp/dbgphplog.txt", "a");
	if (f) {
		fprintf(f, "FREE PTR [%p %p %p] %p %p %p %p\n", &ep->qsplit_listp,
				ep->qsplit_listp.prev, ep->qsplit_listp.next, ep->line_off,
				ep->orig_str, ep->new_str, ep->alias_str);
		fprintf(f, "FREE [%p %p %p] %p (%d) %s, (%d) %s, (%d) %s\n", &ep->qsplit_listp,
				ep->qsplit_listp.prev, ep->qsplit_listp.next, ep->line_off,
				ep->orig_len, ep->orig_str, ep->new_len, ep->new_str, ep->alias_len,
				ep->alias_str);
		fclose(f);
	}
#endif

	if (ep->orig_str)
		free(ep->orig_str);
	if (ep->alias_str)
		free(ep->alias_str);
	if (ep->new_str)
		free(ep->new_str);
	if (ep->line_off)
		free(ep->line_off);
	memset(ep, 0, sizeof(qelem_t));
	list_init(&ep->qsplit_listp);
	free(ep);
	*e = NULL;
}

void
qelem_set_strtype_str(qelem_t *e, int strtype, char *str, int len)
{
	int *strtype_len = NULL;
	char **strtype_str = NULL;
	switch (strtype) {
		case QE_STR_ORIG:
			strtype_len = &e->orig_len;
			strtype_str = &e->orig_str;
			break;
		case QE_STR_ALIAS:
			strtype_len = &e->alias_len;
			strtype_str = &e->alias_str;
			break;
		case QE_STR_NEW:
			strtype_len = &e->new_len;
			strtype_str = &e->new_str;
			break;
	}
	if (len > 0) {
		*strtype_len = len;
		*strtype_str = (char *) malloc(len+1);
		memset(*strtype_str, '\0', len+1);
		memcpy(*strtype_str, str, len);
	}
	//((pANTLR3_UINT8)(*strtype_str))[len] = (ANTLR3_UINT8) -1;
}

void
cleanup_qsplit_list(list_t *qsplit_list)
{
	assert(qsplit_list);

	list_t *qp_head = qsplit_list;
	list_t *qp_it = qp_head->next;
	list_t *qp_next;
	qelem_t *e;
	while (qp_it != qp_head) {
		qp_next = qp_it->next;
		e = list_entry(qp_it, qelem_t, qsplit_listp);
		list_remove(qp_it);
		free_qelem(&e);
		qp_it = qp_next;
	}

	list_init(qsplit_list);
}

void
init_qstr(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) malloc(sizeof(qstr_int_t));
	memset(qint, 0, sizeof(qstr_int_t));
	list_init(&qint->qpos_list);
	list_init(&qint->expected_col_list);
	list_init(&qint->unexpected_col_list);
	list_init(&qint->qsplit_list);
	qint->num_lines = 1; // min 1 line of query
	qint->pol_eval_method = PE_METHOD_DEFAULT;
	q->priv = (void *) qint;
}

qstr_t *
alloc_init_qstr(void)
{
	qstr_t *q = (qstr_t *) malloc(sizeof(qstr_t));
	init_qstr(q);
	return q;
}

void
reset_query_in_qstr(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	cleanup_qsplit_list(&qint->qsplit_list);
	//free(qint->line_off);
	//qint->num_lines = 1;
}

void
reset_qstr(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	if (qint->rewritten_q)
		free(qint->rewritten_q);
	qint->rewritten_q = NULL;

	cleanup_qpos_list(&qint->qpos_list);
	cleanup_qsplit_list(&qint->qsplit_list);
	cleanup_col_sym_list(&qint->expected_col_list);
	cleanup_col_sym_list(&qint->unexpected_col_list);
	if (qint->line_off)
		free(qint->line_off);

	memset(qint, 0, sizeof(qstr_int_t));
	list_init(&qint->qpos_list);
	list_init(&qint->expected_col_list);
	list_init(&qint->unexpected_col_list);
	list_init(&qint->qsplit_list);
	qint->num_lines = 1;
	qint->pol_eval_method = PE_METHOD_DEFAULT;
}

void
cleanup_qstr(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	if (qint->rewritten_q)
		free(qint->rewritten_q);
	qint->rewritten_q = NULL;

	cleanup_qpos_list(&qint->qpos_list);
	cleanup_qsplit_list(&qint->qsplit_list);
	cleanup_col_sym_list(&qint->expected_col_list);
	cleanup_col_sym_list(&qint->unexpected_col_list);
	free(qint->line_off);

	memset(qint, 0, sizeof(qstr_int_t));
	list_init(&qint->qpos_list);
	list_init(&qint->expected_col_list);
	list_init(&qint->unexpected_col_list);
	list_init(&qint->qsplit_list);
	free(qint);
	q->priv = NULL;
}

void
free_qstr(qstr_t **q)
{
	if (!q || !(*q))
		return;

	qstr_t *qq = *q;
	cleanup_qstr(qq);
	free(qq);
	*q = NULL;
}

list_t *
qstr_get_expected_col_list(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	return &qint->expected_col_list;
}

list_t *
qstr_get_unexpected_col_list(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	return &qint->unexpected_col_list;
}

list_t *
qstr_get_split_list(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	return &qint->qsplit_list;
}

int8_t
qstr_get_pol_eval_method(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	return qint->pol_eval_method;
}

static void
add_line_off(int **line_off_arr, int *n_line, int *max_line, int off)
{
	int *line_off = *line_off_arr;
	int max_lines = *max_line;
	int n_lines = *n_line;
	while (n_lines >= max_lines) {
		int *new_line_off = 
			(int *) realloc((char *) line_off, (max_lines*2*sizeof(int)));
		max_lines *= 2;
		line_off = new_line_off;
	}

	line_off[n_lines++] = off;
	*n_line = n_lines;
	*max_line = max_lines;
	*line_off_arr = line_off;
}

static void
set_query_line_off(int **line_off_arr, int *n_line, int *max_line, 
		const char *text, size_t length)
{
	int i = 0;
	char c;
	for (i = 0; i < length; i++) {
		c = text[i];
		if (c == '\n') {
			i++;
			add_line_off(line_off_arr, n_line, max_line, i);
		}
	}
}

void
qstr_add_expected_col(qstr_t *q, col_sym_t *cs)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	list_insert(&qint->expected_col_list, &cs->col_sym_listp);
}

void
qstr_add_unexpected_col(qstr_t *q, col_sym_t *cs)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	list_insert(&qint->unexpected_col_list, &cs->col_sym_listp);
}

// allocated by the caller
void
qstr_set_orig_query(qstr_t *q, const char *text, size_t length, int pe_method)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	qint->orig_q = (char *) text;
	qint->orig_qlen = length;
	qint->max_lines = 10;
	qint->pol_eval_method = pe_method;
	qint->line_off = (int *) malloc(sizeof(int) * qint->max_lines);
	int i;
	for (i = 0; i < qint->max_lines; i++)
		qint->line_off[i] = 0;
	//memset(qint->line_off, 0, sizeof(int) * qint->max_lines);
	set_query_line_off(&qint->line_off, &qint->num_lines, &qint->max_lines, 
			text, length);
}

// allocated by the caller
void
qstr_set_rewritten_query(qstr_t *q, char *text, size_t length)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	qint->rewritten_q = text;
	//qint->rewritten_q = (char *) malloc(length);
	//memset(qint->rewritten_q, 0, length);
	//memcpy(qint->rewritten_q, text, length-1);
	qint->rewritten_qlen = length;
}

void
qstr_free_rewritten_query(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	if (qint->rewritten_q)
		free(qint->rewritten_q);
	qint->rewritten_q = NULL;
	qint->rewritten_qlen = 0;
}

char *
qstr_get_orig_query(qstr_t *q, int *length)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	*length = qint->orig_qlen;
	return qint->orig_q;
}

char *
qstr_get_rewritten_query(qstr_t *q, int *length)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	*length = qint->rewritten_qlen;
	return qint->rewritten_q;
}

qelem_t *
get_next_elem_type(list_t *head, list_t **it, int elem_type)
{
	if (!it)
		return NULL;

	if (list_empty(head))
		return NULL;

	list_t *it_p = *it;
	if (!it_p) {
		it_p = head->next;
	} else {
		it_p = it_p->next;
	}

	qelem_t *e = NULL;
	int found = 0;
	while (it_p != head) {
		e = list_entry(it_p, qelem_t, qsplit_listp);
		if (e->elem_type == elem_type) {
			found = 1;
			break;
		}
		it_p = it_p->next;
	}

	if (found) {
		*it = it_p;
		return e;
	}
	return NULL;
}

void
gen_query(list_t *query_list, list_t *qsplit_list, char **q, int *qlen, int db_type)
{
	qelem_t *e = NULL, *e_param = NULL;
	char *new_q = NULL, *ptr = NULL;
	int new_qlen = 0;

	/*
	 * bugfix: splice parameters from new application query into cached query list
	 */
	int qsplit_count = 0, query_count = 0, ignore_qsplit = 0;
	list_t *qsplit_it = NULL;
	if (!qsplit_list) {
		ignore_qsplit = 1;
	}
	FILE *f = NULL;
	
	list_for_each_entry(e, query_list, qsplit_listp) {
		switch (e->elem_type) {
			case QE_DEFAULT:
#if DEBUG
				printf("%d: orig: %s, len: %d (%d)\n", e->elem_type, e->orig_str, 
						e->orig_len, strlen(e->orig_str));
#endif
				new_qlen += e->orig_len;
				break;
				
			case QE_POLICY:
#if DEBUG
				printf("%d: new: %s, len: %d (%d)\n", e->elem_type, e->new_str, 
						e->new_len, strlen(e->new_str));
#endif
				new_qlen += e->new_len;
				if (e->alias_str && e->alias_len) {
#if DEBUG
					printf("%d: alias: %s, len: %d (%d)\n", e->elem_type, e->alias_str, 
							e->alias_len, strlen(e->alias_str));
#endif
					new_qlen += e->alias_len;
					//new_qlen += 1; // for space after alias
				} else {
#if DEBUG
					printf("%d: orig: %s, len: %d (%d)\n", e->elem_type, e->orig_str, 
							e->orig_len, strlen(e->orig_str));
#endif
					new_qlen += e->orig_len;
				}
				break;

			case QE_SYNTAX:
#if DEBUG
				printf("%d: orig: %s, len: %d(%d)\n", e->elem_type, e->new_str,
						e->new_len, strlen(e->new_str));
#endif
				new_qlen += e->new_len;
				break;

			case QE_PLC:
				if (ignore_qsplit) {
					new_qlen += e->new_len;
#if DEBUG
				printf("%d: new: %s, len: %d (%d)\n", e->elem_type, e->new_str, 
						e->new_len, strlen(e->new_str));
#endif
				} else {
					e_param = get_next_elem_type(qsplit_list, &qsplit_it, QE_PLC);
					if (!e_param)
						assert(0); // parameters in source string have finished
					new_qlen += e_param->new_len;
#if DEBUG
				printf("%d: new: %s, len: %d (%d)\n", e->elem_type, e_param->new_str, 
						e_param->new_len, strlen(e_param->new_str));
#endif
				}
				break;

			default:
				assert(0);
		}
	}

	if (db_type == DB_MYSQL)
		new_qlen += 1; // for ";" at the end of the query

	// ======= actually generate query from qsplit elements ======
	new_q = (char *) malloc(new_qlen+1);
	memset(new_q, 0, new_qlen+1);
	ptr = new_q;
	
	qsplit_it = NULL;
	list_for_each_entry(e, query_list, qsplit_listp) {
		switch(e->elem_type) {
			case QE_DEFAULT:
				memcpy(ptr, e->orig_str, e->orig_len);
				ptr += e->orig_len;
				break;

			case QE_POLICY:
				memcpy(ptr, e->new_str, e->new_len);
				ptr += e->new_len;
				if (e->alias_str && e->alias_len) {
					memcpy(ptr, e->alias_str, e->alias_len);
					ptr += e->alias_len;
			//		memcpy(ptr, " ", 1);
			//		ptr += 1;
				} else {
					memcpy(ptr, e->orig_str, e->orig_len);
					ptr += e->orig_len;
				}
				break;

			case QE_SYNTAX:
				memcpy(ptr, e->new_str, e->new_len);
				ptr += e->new_len;
				break;

			case QE_PLC:
				if (ignore_qsplit) {
					memcpy(ptr, e->new_str, e->new_len);
					ptr += e->new_len;
				} else {
					e_param = get_next_elem_type(qsplit_list, &qsplit_it, QE_PLC);
					if (!e_param)
						assert(0); // parameters in source string have finished
					memcpy(ptr, e_param->new_str, e_param->new_len);
					ptr += e_param->new_len;
				}
		}
	}

	if (db_type == DB_MYSQL) {
		memcpy(ptr, ";", 1);
		ptr += 1;
	}

	*q = new_q;
	*qlen = new_qlen+1;
#if DEBUG
	printf("db_type: %d, NEW QUERY (%d) -----\n%s\n", db_type, new_qlen, new_q);
#endif
}

char *
qstr_gen_rewritten_query(qstr_t *q, int *length, int db_type)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	list_t *qsplit_list = &qint->qsplit_list;
	char *new_q = NULL;
	int new_qlen = 0;
	gen_query(qsplit_list, NULL, &new_q, &new_qlen, db_type);
	*length = new_qlen;
	qstr_set_rewritten_query(q, new_q, new_qlen);

	return new_q;
}

static void
set_table_char_pos(qstr_int_t *qint, qpos_t *qp)
{
	char *ptr = qint->orig_q;
	qp->pos = ptr + qp->off; 

	if (qp->alias_off && qp->alias_len) {
		qp->alias_pos = ptr + qp->alias_off;
	}
}

void
qstr_add_table_pos(qstr_t *q, int off, int len, int line, int aoff, int alen, 
		int aline, sym_t *s)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	qpos_t *qp = (qpos_t *) malloc(sizeof(qpos_t));
	memset(qp, 0, sizeof(qpos_t));
	list_init(&qp->qpos_listp);
	// XXX: if query is sent through cmdline, table identifier pos is offset by 1
	// compared to if query is sent as in php string
	int off2 = off ? off : 0;
	int line2 = line > 0 ? line - 1 : 0;
	qp->off = qint->line_off[line2] + off2;
	qp->len = len;
	qp->line = line2;
	int aoff2 = aoff ? aoff : 0;
	int aline2 = aline > 0 ? aline - 1 : 0;
	qp->alias_off = qint->line_off[aline2] + aoff2;
	qp->alias_len = alen;
	qp->alias_line = aline2;
	qp->s = s;
	set_table_char_pos(qint, qp);
	//printf("off: %d, len: %d, pos: %s\n", qp->off, qp->len, qp->pos);
	list_insert_at_tail(&qint->qpos_list, &qp->qpos_listp);
	qint->num_qpos++;
}

list_t *
qstr_get_table_pos_list(qstr_t *q, int *num_qpos)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	*num_qpos = qint->num_qpos;
	return &qint->qpos_list;
}

void
qstr_split_string_at_tables(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	char *orig_q = qint->orig_q;
	list_t *qpos_list = &qint->qpos_list;
	qpos_t *qp = NULL;
	qpos_t *qp_prev = NULL;
	int off = 0, prev_off = 0, alias_off = 0, prev_alias_off = 0;
	char *pos = NULL, *prev_pos = NULL, *alias_pos = NULL, *prev_alias_pos = NULL;

	list_t *qsplit_list = &qint->qsplit_list;
	qelem_t *e1 = NULL, *e2 = NULL;

	int count = 0;
	list_for_each_entry(qp, qpos_list, qpos_listp) {
		e1 = alloc_init_qelem(QE_DEFAULT);
		e2 = alloc_init_qelem(QE_POLICY);

		if (count == 0) {
			off = (qp->line > 0 ? qp->off : qp->off + 1);
			e1->orig_len = off;
			e1->orig_str = (char *) malloc(e1->orig_len+1);
			memset(e1->orig_str, 0, e1->orig_len+1);
			memcpy(e1->orig_str, orig_q, off);
			//memcpy(e1->orig_str + off, " ", 1);
		} else {
			if (qp_prev->alias_off && qp_prev->alias_len) {
				prev_alias_off = (qp_prev->alias_line > 0 ? qp_prev->alias_off :
						qp_prev->alias_off + 1);
				prev_alias_pos = (qp_prev->alias_line > 0 ? qp_prev->alias_pos :
						qp_prev->alias_pos + 1);
				off = (qp->line > 0 ? qp->off : qp->off + 1);
				e1->orig_len = off - (prev_alias_off + qp_prev->alias_len);
				e1->orig_str = (char *) malloc(e1->orig_len+1);
				memset(e1->orig_str, 0, e1->orig_len+1);
				memcpy(e1->orig_str, (prev_alias_pos + qp_prev->alias_len), e1->orig_len);
			} else {
				prev_off = (qp_prev->line > 0 ? qp_prev->off : qp_prev->off + 1);
				prev_pos = (qp_prev->line > 0 ? qp_prev->pos : qp_prev->pos + 1);
				off = (qp->line > 0 ? qp->off : qp->off + 1);
				e1->orig_len = off - (prev_off + qp_prev->len);
				e1->orig_str = (char *) malloc(e1->orig_len+1);
				memset(e1->orig_str, 0, e1->orig_len+1);
				memcpy(e1->orig_str, (prev_pos + qp_prev->len), e1->orig_len);
			}
		}

		set_query_line_off(&e1->line_off, &e1->num_lines, &e1->max_lines,
				e1->orig_str, e1->orig_len);
		list_insert_at_tail(qsplit_list, &e1->qsplit_listp);

		pos = (qp->line > 0 ? qp->pos : qp->pos + 1);
		alias_pos = (qp->alias_line > 0 ? qp->alias_pos : qp->alias_pos + 1);
		e2->orig_len = qp->len;
		e2->orig_str = (char *) malloc(e2->orig_len+1);
		memset(e2->orig_str, 0, e2->orig_len+1);
		memcpy(e2->orig_str, pos, e2->orig_len);
		if (qp->alias_off && qp->alias_len) {
			e2->alias_len = qp->alias_len;
			e2->alias_str = (char *) malloc(e2->alias_len+1);
			memset(e2->alias_str, 0, e2->alias_len+1);
			memcpy(e2->alias_str, alias_pos, e2->alias_len);
		}
		set_query_line_off(&e2->line_off, &e2->num_lines, &e2->max_lines,
				e2->orig_str, e2->orig_len);
		list_insert_at_tail(qsplit_list, &e2->qsplit_listp);

		qp_prev = qp;
#if 0
		printf("e1 (%d): %s\n", e1->orig_len, e1->orig_str);
		printf("e2 (%d): %s\n", e2->orig_len, e2->orig_str);
		if (e2->alias_str && e2->alias_len)
			printf("e2 alias (%d): %s\n", e2->alias_len, e2->alias_str);
		printf("qp, off: %d, len: %d, line: %d, pos: %p, %s\n", qp->off, qp->len, 
				qp->line, qp->pos, qp->pos);
		if (qp->alias_off * qp->alias_len)
			printf("qp alias, off: %d, len: %d, line: %d, pos: %p, %s\n", qp->alias_off, 
					qp->alias_len, qp->alias_line, qp->alias_pos, qp->alias_pos);
#endif
		count++;
	}

	if ((qp_prev->alias_off && qp_prev->alias_len && 
			(qp_prev->alias_pos + qp_prev->alias_len >= orig_q + qint->orig_qlen)) ||
		 (qp_prev->pos + qp_prev->len >= orig_q + qint->orig_qlen))
		return;
	
	e1 = alloc_init_qelem(QE_DEFAULT);
	if (qp_prev->alias_off && qp_prev->alias_len) {
		alias_off = (qp_prev->alias_line > 0 ? qp_prev->alias_off : qp_prev->alias_off + 1);
		alias_pos = (qp_prev->alias_line > 0 ? qp_prev->alias_pos : qp_prev->alias_pos + 1);
		e1->orig_len = qint->orig_qlen - (alias_off + qp_prev->alias_len);
		e1->orig_str = (char *) malloc(e1->orig_len+1);
		memset(e1->orig_str, 0, e1->orig_len+1);
		memcpy(e1->orig_str, alias_pos + qp_prev->alias_len, e1->orig_len);
	} else {
		e1->orig_len = qint->orig_qlen - (off + qp_prev->len);
		e1->orig_str = (char *) malloc(e1->orig_len+1);
		memset(e1->orig_str, 0, e1->orig_len+1);
		memcpy(e1->orig_str, pos + qp_prev->len, e1->orig_len);
	}
	set_query_line_off(&e1->line_off, &e1->num_lines, &e1->max_lines,
			e1->orig_str, e1->orig_len);
	list_insert_at_tail(qsplit_list, &e1->qsplit_listp);
}

void
qstr_set_first_split_string(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	list_t *qsplit_list = &qint->qsplit_list;
	qelem_t *e = NULL, *prev_e = NULL;
	if (list_empty(qsplit_list)) {
		e = alloc_init_qelem(QE_DEFAULT);
		qelem_set_strtype_str(e, QE_STR_ORIG, qint->orig_q, qint->orig_qlen);
		set_query_line_off(&e->line_off, &e->num_lines, &e->max_lines, e->orig_str,
				e->orig_len);
		list_insert(qsplit_list, &e->qsplit_listp);
	}
}

void
qstr_parameterize_split_string(qstr_t *q)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	list_t *qsplit_list = &qint->qsplit_list;
	qelem_t *e = NULL, *prev_e = NULL;
	param_t *p = NULL, *prev_p = NULL;
	qelem_t *e_left = NULL, *e_new = NULL, *e_right = NULL;
	int off = 0, length = 0, prev_off = 0, prev_length = 0;
	char *pos = NULL, *prev_pos = NULL;
	int to_skip = 0;
	int num_params = 0;
	void *p_scan = NULL;
	int i;

	list_t param_list;
	list_init(&param_list);

	list_for_each_entry(e, qsplit_list, qsplit_listp) {
		to_skip = 0;
#if DEBUG
		printf("orig (%d): %s, new (%d): %s, alias (%d): %s\n", e->orig_len,
				(e->orig_len ? e->orig_str : "nil"), e->new_len,
				(e->new_len ? e->new_str : "nil"), e->alias_len,
				(e->alias_len ? e->alias_str : "nil"));
		qstr_print_split_elem(e);
#endif

		if (e->elem_type == QE_PLC || e->elem_type == QE_POLICY)
			continue;

#if DEBUG
		if (e->elem_type == QE_SYNTAX) {
			printf("###########\n");
			printf("orig (%d): %s, new (%d): %s, alias (%d): %s\n", e->orig_len,
					(e->orig_len ? e->orig_str : "nil"), e->new_len,
					(e->new_len ? e->new_str : "nil"), e->alias_len,
					(e->alias_len ? e->alias_str : "nil"));
		}
#endif

		if (!(e->orig_str && e->orig_len)) {
			continue;
		} 

		p_scan = qapla_get_parameters(e->orig_str, e->orig_len, &param_list);
		e_left = e;
		list_for_each_entry(p, &param_list, param_listp) {
			e_new = alloc_init_qelem(QE_PLC);
			qelem_set_strtype_str(e_new, QE_STR_NEW, p->str, p->len);
			if (p->line > 1)
				off = e->line_off[p->line-1] + p->position;
			else
				off = p->position + 1;
			length = e->orig_len - (off + p->len);
			if (length > 0) {
				e_right = alloc_init_qelem(e_left->elem_type);
				pos = e->orig_str + off + p->len;
				qelem_set_strtype_str(e_right, QE_STR_ORIG, pos, length);
			}
			length = e->orig_len - off;
			memset(e_left->orig_str+off, 0, length);
			e_left->orig_len = e->orig_len - length;

			if (e_left->elem_type == QE_SYNTAX) {
				char *new_pos = NULL;
				int new_len = 0;
				char new_buf[16];
				memset(new_buf, 0, 16);
				memcpy(new_buf, p->str, p->len);
				int new_off_ret = find_haystack_needle_reverse(e_left->new_str,
						e_left->new_str+e_left->new_len, new_buf, &new_pos);
				if (new_off_ret) {
					new_len = (e->new_str + e->new_len) - (new_pos + p->len);
					if (new_len > 0) {
						if (!e_right) {
							e_right = alloc_init_qelem(e_left->elem_type);
						}
						if (!e_right)
							assert(0);
						qelem_set_strtype_str(e_right, QE_STR_NEW, new_pos+p->len, new_len);
					}
					new_len = e_left->new_str + e_left->new_len - new_pos;
					memset(new_pos, 0, new_len);
					e_left->new_len = e->new_len - new_len;		
					set_query_line_off(&e_right->line_off, &e_right->num_lines, &e->max_lines,
							e->orig_str, e->orig_len);

					list_insert(&e_left->qsplit_listp, &e_new->qsplit_listp);
					if (e_right)
						list_insert(&e_new->qsplit_listp, &e_right->qsplit_listp);
					to_skip++;

				} else {
					// this case occurs when there were multiple syntactic changes on a term
					// e.g. group_concat(concat(..., parameter, ...)) translated to
					// [listagg(,] => [concat(concat(..., parameter), ...)] => ...
					// and parameter is compared with the first elem in the list, which has type syntax
					// in this case, we simply keep the elem and move forward
					// TODO: better handling of such elements -- 
					// requires some more state in the elem structure
				}
			} else {
				list_insert(&e_left->qsplit_listp, &e_new->qsplit_listp);
				if (e_right)
					list_insert(&e_new->qsplit_listp, &e_right->qsplit_listp);
				to_skip++;
			}

#if DEBUG
			printf(" LEFT TYPE %d [%p %p %p]: orig (%d): %s, new (%d): %s, alias (%d): %s\n", 
					e_left->elem_type,
					&e_left->qsplit_listp, e_left->qsplit_listp.prev, e->qsplit_listp.next,
					e->orig_len,
					(e->orig_len ? e->orig_str : "nil"), e->new_len,
					(e->new_len ? e->new_str : "nil"), e->alias_len,
					(e->alias_len ? e->alias_str : "nil"));
			if (e_new) {
				printf(" NEW TYPE %d [%p %p %p]: orig (%d): %s, new (%d): %s, alias (%d): %s\n", 
						e_new->elem_type,
					&e_new->qsplit_listp, e_new->qsplit_listp.prev, e_new->qsplit_listp.next,	
					e_new->orig_len,
					(e_new->orig_len ? e_new->orig_str : "nil"), e_new->new_len,
					(e_new->new_len ? e_new->new_str : "nil"), e_new->alias_len,
					(e_new->alias_len ? e_new->alias_str : "nil"));
			}
			if (e_right) {
				printf(" RIGHT TYPE %d [%p %p %p]: orig (%d): %s, new (%d): %s, alias (%d): %s\n", 
						e_right->elem_type,
					&e_right->qsplit_listp, e_right->qsplit_listp.prev, 
					e_right->qsplit_listp.next, e_right->orig_len,
					(e_right->orig_len ? e_right->orig_str : "nil"), e_right->new_len,
					(e_right->new_len ? e_right->new_str : "nil"), e_right->alias_len,
					(e_right->alias_len ? e_right->alias_str : "nil"));
			}
#endif

			e_right = NULL;
		}

		for (i = 0; i < to_skip*2; i++) {
			e = list_next_entry(e, qsplit_listp);
		}
		prev_e = e;

		list_t *p_head = &param_list;
		list_t *p_it = p_head->next;
		list_t *p_next;
		while (p_it != p_head) {
			p = list_entry(p_it, param_t, param_listp);
			p_next = p_it->next;
			list_remove(&p->param_listp);
			free(p);
			p_it = p_next;
		}
		qapla_reset_scanner(p_scan);
	}
}

void
qstr_gen_parameterized_query(qstr_t *q, char **q_buf, int *q_len)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	list_t *qsplit_list = &qint->qsplit_list;
	qelem_t *e = NULL;
	char *buf = NULL, *ptr = NULL;
	int len = 0;
	list_for_each_entry(e, qsplit_list, qsplit_listp) {
		if (e->elem_type == QE_PLC)
			continue;

		len += e->orig_len;
		if (e->alias_str && e->alias_len)
			len += e->alias_len;
	}

	buf = (char *) malloc(len+1);
	memset(buf, 0, len+1);
	ptr = buf;
	list_for_each_entry(e, qsplit_list, qsplit_listp) {
		if (e->elem_type == QE_PLC)
			continue;
		memcpy(ptr, e->orig_str, e->orig_len);
		ptr += e->orig_len;
		if (e->alias_str && e->alias_len) {
			memcpy(ptr, e->alias_str, e->alias_len);
			ptr += e->alias_len;
		}
	}

	*q_buf = buf;
	*q_len = len;
}

void
qstr_print_split_elem(qelem_t *e)
{
	printf("type %d: [%p %p %p], ", e->elem_type, &e->qsplit_listp,
			e->qsplit_listp.prev, e->qsplit_listp.next);
	switch (e->elem_type) {
		case QE_DEFAULT:
			printf("%s .. ", e->orig_str);
			break;

		case QE_POLICY:
			if (e->new_str) {
				printf("%s .. ", e->new_str);
				if (e->alias_str)
					printf("%s .. ", e->alias_str);
				else
					printf("%s .. ", e->orig_str);
			} else {
				printf("%s .. ", e->orig_str);
				if (e->alias_str)
					printf("%s .. ", e->alias_str);
			}
			break;

		case QE_SYNTAX:
		case QE_PLC:
			printf("%s .. ", e->new_str);
			break;

		default:
			assert(0);
	}

	if (e->num_lines > 1)
		printf("#lines: %d\n", e->num_lines);
	else
		printf("\n");

}

void
qstr_print_split_list(list_t *qpos_split)
{
	int cnt = 0;
	qelem_t *e = NULL;
	list_for_each_entry(e, qpos_split, qsplit_listp) {
		qstr_print_split_elem(e);
		cnt++;
	}
	printf("\n#e: %d\n", cnt);
}

void
qstr_print_split_string(qstr_t *q)
{
	int cnt = 0;
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	list_t *qpos_split = &qint->qsplit_list;
	qelem_t *e = NULL;
	list_for_each_entry(e, qpos_split, qsplit_listp) {
		qstr_print_split_elem(e);
		cnt++;
	}
	printf("\n#e: %d\n", cnt);
}

void
qstr_print_table_pos(qstr_t *q, FILE *f)
{
	qstr_int_t *qint = (qstr_int_t *) q->priv;
	list_t *qpos_head = &qint->qpos_list;
	qpos_t *qp;
#ifndef MAX_NAME_LEN
#define MAX_NAME_LEN 128
#endif
	char tname[MAX_NAME_LEN];
	char alias[MAX_NAME_LEN];
	list_for_each_entry(qp, qpos_head, qpos_listp) {
		memset(tname, 0, MAX_NAME_LEN);
		memset(alias, 0, MAX_NAME_LEN);
		if (qp->off)
			snprintf(tname, qp->len+1, "%s", qp->pos);
		else
			sprintf(tname, "NULL");
		if (qp->alias_off)
			snprintf(alias, qp->alias_len+1, "%s", qp->alias_pos);
		else
			sprintf(alias, "NULL");

		fprintf(f, "%s[%d] => off: %d, len: %d, line: %d, pos: %s "
				"alias(off: %d, len: %d, line: %d, pos: %s)\n", 
				sym_ptr_field(qp, db_tname), sym_ptr_field(qp, db_tid), 
				qp->off, qp->len, qp->line, tname,
				qp->alias_off, qp->alias_len, qp->alias_line, alias);
	}
}
