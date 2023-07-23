/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QUERY_SYMBOL_H__
#define __QUERY_SYMBOL_H__

#include "utils/list.h"
#include "common/sym.h"
#include "common/col_sym.h"

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct symbol {
		sym_t s;
		list_t col_sym_list;
		list_t symbol_listp;
	} symbol_t;

	typedef int (*check_fn) (col_sym_t *to_insert, col_sym_t *exists);

	enum {
		CHECK_GEN = 0,
		CHECK_ONLY_CID,
	};

	void init_symbol(symbol_t *sym);
	void set_symbol(symbol_t *sym, char *name, int id, int type);
	symbol_t *dup_symbol(symbol_t *src);
	void cleanup_symbol(symbol_t *sym);
	void free_symbol(symbol_t **sym);
	void cleanup_symbol_list(list_t *sym_list);

	void print_symbol(symbol_t *sym, FILE *f);
	void print_symbol_list(list_t *sym_list, FILE *f);

	col_sym_t *insert_col_sym_to_sym_list(symbol_t *sym, col_sym_t *cs);
	col_sym_t *insert_col_sym_to_sym_list_fn(symbol_t *sym, col_sym_t *cs, int check_type);
	col_sym_t *get_col_sym_by_expr_type_in_sym(symbol_t *sym, int expr, list_t **next);

#ifdef __cplusplus
}
#endif

#endif /* __QUERY_SYMBOL_H__ */
