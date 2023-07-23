/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#include <stdint.h>
#include "types.h"

uint8_t
type_int_eq(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize)
{
	uint64_t *lval = (uint64_t *) lhs;
	uint64_t *rval = (uint64_t *) rhs;
	return (!(*lval == *rval));
}

uint8_t
type_int_gt(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize)
{
	uint64_t *lval = (uint64_t *) lhs;
	uint64_t *rval = (uint64_t *) rhs;
	return (!(*lval > *rval));
}

uint8_t
type_int_ge(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize)
{
	uint64_t *lval = (uint64_t *) lhs;
	uint64_t *rval = (uint64_t *) rhs;
	return (!(*lval >= *rval));
}

uint8_t
type_int_lt(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize)
{
	uint64_t *lval = (uint64_t *) lhs;
	uint64_t *rval = (uint64_t *) rhs;
	return (!(*lval < *rval));
}

uint8_t
type_int_le(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize)
{
	uint64_t *lval = (uint64_t *) lhs;
	uint64_t *rval = (uint64_t *) rhs;
	return (!(*lval <= *rval));
}

