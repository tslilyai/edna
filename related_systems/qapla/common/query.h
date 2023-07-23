/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QUERY_H__
#define __QUERY_H__

#include "utils/list.h"
#include "sym.h"
#include "col_sym.h"

#ifdef __cplusplus
extern "C" {
#endif

	enum {
		QE_STR_ORIG = 0,
		QE_STR_ALIAS,
		QE_STR_NEW,
		MAX_QE_STR
	};

	enum {
		QE_DEFAULT = 0,
		QE_POLICY,
		QE_SYNTAX,
		QE_PLC, // for query parameters
		MAX_QE
	};

	typedef struct qelem {
		char *orig_str;
		char *alias_str; // only for QE_POLICY type
		char *new_str;
		int orig_len;
		int alias_len; // only for QE_POLICY type
		int new_len;
		int elem_type;
		int num_lines;
		int max_lines;
		int *line_off;
		list_t qsplit_listp;
	} qelem_t;

	// all pointers are read-only, should not be freed from this structure
	typedef struct qpos {
		int off; // -1 if null
		int len; // -1 if null
		int line;
		int alias_off; // -1 if null
		int alias_len; // -1 if null
		int alias_line;
		char *pos;
		char *alias_pos;
		sym_t *s;
		list_t qpos_listp;
	} qpos_t;

	typedef struct qstr {
		void *priv;
	} qstr_t;

	qelem_t *alloc_init_qelem(int elem_type);
	qelem_t *clone_qelem(qelem_t *e_src);
	void clone_qelem_list(list_t *src_list, list_t *dst_list);
	void free_qelem(qelem_t **e);
	void qelem_set_strtype_str(qelem_t *e, int type, char *str, int len);

	void init_qstr(qstr_t *q);
	qstr_t *alloc_init_qstr();
	void reset_query_in_qstr(qstr_t *q);
	void reset_qstr(qstr_t *q);
	void cleanup_qstr(qstr_t *q);
	void free_qstr(qstr_t **q);
	int8_t qstr_get_pol_eval_method(qstr_t *q);

	void qstr_add_expected_col(qstr_t *q, col_sym_t *cs);
	void qstr_add_unexpected_col(qstr_t *q, col_sym_t *cs);
	list_t *qstr_get_expected_col_list(qstr_t *q);
	list_t *qstr_get_unexpected_col_list(qstr_t *q);

	void qstr_set_orig_query(qstr_t *q, const char *text, size_t length, int pe_method);
	char *qstr_get_orig_query(qstr_t *q, int *length);
	void qstr_set_rewritten_query(qstr_t *q, char *text, size_t length);
	void qstr_free_rewritten_query(qstr_t *q);
	char *qstr_get_rewritten_query(qstr_t *q, int *length);
	char *qstr_gen_rewritten_query(qstr_t *q, int *length, int db_type);
	
	void qstr_add_table_pos(qstr_t *q, int off, int len, int line, int aoff, int alen, 
			int aline, sym_t *s);
	list_t *qstr_get_table_pos_list(qstr_t *q, int *num_qpos);
	void qstr_print_table_pos(qstr_t *q, FILE *f);

	void qstr_split_string_at_tables(qstr_t *q);
	void qstr_set_first_split_string(qstr_t *q);
	void qstr_parameterize_split_string(qstr_t *q);
	void qstr_gen_parameterized_query(qstr_t *q, char **q_buf, int *q_len);
	void gen_query(list_t *query_list, list_t *qsplit_list, char **q, int *qlen, int db_type);
	list_t *qstr_get_split_list(qstr_t *q);
	void cleanup_qsplit_list(list_t *qsplit_list);
	void qstr_print_split_elem(qelem_t *e);
	void qstr_print_split_list(list_t *qsplit_list);
	void qstr_print_split_string(qstr_t *q);

#ifdef __cplusplus
}
#endif

#endif /* __QUERY_H__ */
