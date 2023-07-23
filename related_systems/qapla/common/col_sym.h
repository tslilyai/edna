/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QUERY_COL_SYM_H__
#define __QUERY_COL_SYM_H__

#include <stdint.h>
#include "utils/list.h"
#include "common/db.h"
#include "common/sym.h"
#include "sym_xform.h"

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct col_sym {
		sym_t s;
		list_t xform_list;
		list_t col_sym_listp;
		uint8_t is_expected;
		uint8_t is_set;
		void *ptr;
		int ptr_size;
	} col_sym_t;

	void generate_col_sym_alias(char *abuf, char *tname, char *cname, list_t *xf_list);
	void init_col_sym(col_sym_t *cs);
	void set_col_sym_tid_cid(col_sym_t *cs, int tid, int cid);
	col_sym_t *alloc_init_col_sym_from_schema(db_t *schema, char *tname, char *cname);
	col_sym_t *alloc_init_col_sym(char *alias, char *tname, char *cname, 
			list_t *xf_list, int expr);
	col_sym_t *dup_col_sym(col_sym_t *src);
	int compare_col_sym(col_sym_t *c1, col_sym_t *c2, int exact_match);
	int exists_col_sym_in_list(col_sym_t *cs, list_t *cs_list, int exact_match,
			col_sym_t **match_cs);
	void cleanup_col_sym(col_sym_t *cs);
	void free_col_sym(col_sym_t **cs);
	void cleanup_col_sym_list(list_t *cs_list);

	void add_rev_xf_list_to_col_sym_at_head(col_sym_t *cs, list_t *xf_list);
	void print_col_sym(col_sym_t *cs, FILE *f);
	void print_col_sym_list(list_t *cs_list, FILE *f);

#ifdef __cplusplus
}
#endif

#endif /* __QUERY_COL_SYM_H__ */
