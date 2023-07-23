/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QUERY_CONTEXT_H__
#define __QUERY_CONTEXT_H__

#include "utils/list.h"
#include "common/db.h"
#include "common/col_sym.h"
#include "query_symbol.h"

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct stack {
		list_t list;
		list_t *tos;
	} stack_head_t;

	typedef struct parser_context {
		db_t *schema; // read-only pointer to schema in query info
		stack_head_t context_stack;
		int num_context;
		int num_symbol;
	} parser_context_t;

	struct context;
	typedef struct context {
		int8_t id;
		int8_t visited;
		int8_t is_aggr_query;
		struct context * parent;
		struct context *children[10];
		int8_t child_context_idx;
		stack_head_t *context_stackp;
		list_t symbol_list[NUM_SYM_TYPE];
		list_t context_listp;
		list_t *prev_tos;
	} context_t;

	void init_parser_context(parser_context_t *pc);
	void cleanup_parser_context(parser_context_t *pc);

	void init_context(context_t *c);
	context_t *alloc_init_context(parser_context_t *pc);
	void cleanup_context(context_t *c);
	void free_context(context_t **c);

	void set_context_parent(context_t *context, context_t *parent);
	context_t *get_curr_context(parser_context_t *pc);
	context_t *get_first_context(parser_context_t *pc);

	symbol_t *alloc_init_symbol(parser_context_t *pc, char *name, int type);
	void insert_sym_to_context(context_t *c, symbol_t *sym);
	symbol_t *get_symbol_by_name_in_context_list(context_t *c, char *name, int list);
	int dup_context_symbol_list(context_t *c, int list, list_t *dup_list);
	symbol_t *get_next_symbol_by_expr_type_in_context_list(context_t *c, int expr, 
			int list, list_t **next);
	int get_schema_tid_for_context_col_sym(context_t *context, db_t *schema, 
			char *cname);
	void resolve_col_sym_db_info(parser_context_t *pc);
	void insert_resolved_col_sym_to_sym_list(parser_context_t *pc, symbol_t *sym, 
		char *tname, char *cname, list_t *xf_list, int expr_type);
#ifdef __cplusplus
}
#endif

#endif /* __QUERY_CONTEXT_H__ */
