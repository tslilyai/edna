/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QUERY_SYM_XFORM_H__
#define __QUERY_SYM_XFORM_H__

#include "utils/list.h"

#define CMP_XF_SUCCESS 0
#define CMP_XF_FAILURE 1
#define CMP_XF_TRIVIAL 2

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct transform {
		int fn;
		list_t xform_listp;
	} transform_t;

	transform_t *init_transform(int func);
	void dup_transform(list_t *dst_xf_list, list_t *src_xf_list);
	void free_transform(transform_t **xf);
	void free_transform_list(list_t *xf_list);
	int cmp_transform(list_t *xf_list1, list_t *xf_list2);
	void get_xform_list(char *print_buf, char *prefix, list_t *xf_list, char *suffix);

#ifdef __cplusplus
}
#endif

#endif /* __QUERY_SYM_XFORM_H__ */
