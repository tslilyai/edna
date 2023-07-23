/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#include <string.h>
#include <stdint.h>
#include "types.h"
	
uint8_t
type_varlen_eq(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize)
{
	char *lval = (char *) lhs;
	char *rval = (char *) rhs;
	if (lhsize != rhsize)
		return TYPE_FN_FAILURE;

	if (strncmp(lval, rval, lhsize) == 0)
		return TYPE_FN_SUCCESS;

	return TYPE_FN_FAILURE;
}

// lhs > rhs
uint8_t
type_varlen_gt(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize)
{
	char *lval = (char *) lhs;
	char *rval = (char *) rhs;
	uint32_t len = (lhsize <= rhsize) ? lhsize : rhsize;

	uint32_t ret = strncmp(lval, rval, len);
	if (ret != 0)
		return TYPE_FN_FAILURE;

	if (lhsize > rhsize)
		return TYPE_FN_SUCCESS;

	return TYPE_FN_FAILURE;
}

uint8_t
type_varlen_ge(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize)
{
	char *lval = (char *) lhs;
	char *rval = (char *) rhs;
	uint32_t len = (lhsize <= rhsize) ? lhsize : rhsize;

	uint32_t ret = strncmp(lval, rval, len);
	if (ret != 0)
		return TYPE_FN_FAILURE;

	if (lhsize >= rhsize)
		return TYPE_FN_SUCCESS;

	return TYPE_FN_FAILURE;
}

uint8_t
type_varlen_lt(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize)
{
	char *lval = (char *) lhs;
	char *rval = (char *) rhs;
	uint32_t len = (lhsize <= rhsize) ? lhsize : rhsize;

	uint32_t ret = strncmp(lval, rval, len);
	if (ret != 0)
		return TYPE_FN_FAILURE;

	if (lhsize < rhsize)
		return TYPE_FN_SUCCESS;

	return TYPE_FN_FAILURE;
}

uint8_t
type_varlen_le(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize)
{
	char *lval = (char *) lhs;
	char *rval = (char *) rhs;
	uint32_t len = (lhsize <= rhsize) ? lhsize : rhsize;

	uint32_t ret = strncmp(lval, rval, len);
	if (ret != 0)
		return TYPE_FN_FAILURE;

	if (lhsize <= rhsize)
		return TYPE_FN_SUCCESS;

	return TYPE_FN_FAILURE;
}
