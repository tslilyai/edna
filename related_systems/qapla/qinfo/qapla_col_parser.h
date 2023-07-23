/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QAPLA_COL_PARSER_H__
#define __QAPLA_COL_PARSER_H__

#include <antlr3.h>
#include <iostream>

#ifdef __cplusplus
extern "C" {
#endif

	void traverse_ast_select(void *qi, pANTLR3_BASE_TREE tree, 
			ANTLR3_UINT32 *idx, int from_subquery);
	void do_traverse_ast_resolve(void *qi, void * tree_p);
	void resolve_symbols(void *qi);
	std::string get_query_from_ast_int(std::string result, pANTLR3_BASE_TREE tree);

#ifdef __cplusplus
}
#endif

#endif /* __QAPLA_COL_PARSER_H__ */
