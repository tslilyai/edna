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
#include <string.h>

#include "common/db.h"
#include "sym_xform.h"

const char *transform_func_name[] = { 
	"count", 
	"sum", 
	"avg", 
	"stddev", 
	"max", 
	"min",
	"affil",
	"topic"
};

static char *
get_transform_func_name(int aggr_func)
{
	if (aggr_func < 0 || aggr_func >= MAX_AGGR_FUNC)
		return (char *) "";

	return (char *) transform_func_name[aggr_func];
}

transform_t *
init_transform(int function)
{
	transform_t *tf = (transform_t *) malloc(sizeof(transform_t));
	tf->fn = function;
	list_init(&tf->xform_listp);
	return tf;
}

void 
dup_transform(list_t *dst_xf_list, list_t *src_xf_list)
{
	list_t *xf_head = src_xf_list;
	transform_t *xf, *new_xf;
	list_for_each_entry(xf, xf_head, xform_listp) {
		new_xf = init_transform(xf->fn);
		list_insert_at_tail(dst_xf_list, &new_xf->xform_listp);
	}
}

void
free_transform(transform_t **xf)
{
	if (!xf || !(*xf))
		return;

	transform_t *tf = *xf;
	list_init(&tf->xform_listp);
	free(tf);
	*xf = NULL;
}

void
free_transform_list(list_t *xf_list)
{
	list_t *xf_head = xf_list;
	list_t *xf_it = xf_head->next;
	list_t *xf_next;
	transform_t *xf;
	while (xf_it != xf_head) {
		xf_next = xf_it->next;
		xf = list_entry(xf_it, transform_t, xform_listp);
		list_remove(xf_it);
		free_transform(&xf);
		xf_it = xf_next;
	}
}

static int
cmp_xf_trivial(transform_t *xf1, transform_t *xf2)
{
	if (!xf1 && !xf2)
		return CMP_XF_SUCCESS;

	if ((xf1 && !xf2) || (!xf1 && xf2))
		return CMP_XF_FAILURE;

	return CMP_XF_TRIVIAL;
}

static int
cmp_xf(transform_t *xf1, transform_t *xf2)
{
	if (xf1->fn == xf2->fn)
		return CMP_XF_SUCCESS;

	return CMP_XF_FAILURE;
}

int
cmp_transform(list_t *xf_list1, list_t *xf_list2)
{
	int ret;
	list_t *xf_head1 = xf_list1;
	list_t *xf_head2 = xf_list2;
	list_t *xf_it1 = xf_head1->next;
	list_t *xf_it2 = xf_head2->next;
	transform_t *xf1, *xf2;
	while (xf_it1 != xf_head1 && xf_it2 != xf_head2) {
		xf1 = list_entry(xf_it1, transform_t, xform_listp);
		xf2 = list_entry(xf_it2, transform_t, xform_listp);
		ret = cmp_xf_trivial(xf1, xf2);
		if (ret < CMP_XF_TRIVIAL)
			return ret;

		ret = cmp_xf(xf1, xf2);
		if (ret != CMP_XF_SUCCESS)
			return ret;

		xf_it1 = xf_it1->next;
		xf_it2 = xf_it2->next;
	}

	if (xf_it1 == xf_head1 && xf_it2 == xf_head2)
		return CMP_XF_SUCCESS;

	return CMP_XF_FAILURE;
}

void 
get_xform_list(char *print_buf, char *prefix, list_t *xf_list, char *suffix)
{
	char *ptr = print_buf;
	char *fn_name;
	int nparen = 0, iparen;
	list_t *xf_head = xf_list;
	transform_t *xf;
	if (prefix) {
		sprintf(ptr, "%s ", prefix);
		ptr += strlen(prefix)+1;
	}
	list_for_each_entry(xf, xf_head, xform_listp) {
		fn_name = get_transform_func_name(xf->fn);
		sprintf(ptr, "%s(", fn_name);
		ptr += strlen(fn_name)+1;
		nparen++;
	}
	if (suffix) {
		sprintf(ptr, "%s", suffix);
		ptr += strlen(suffix);
	}
	for (iparen = 0; iparen < nparen; iparen++) {
		sprintf(ptr, ")");
		ptr+=1;
	}
}

