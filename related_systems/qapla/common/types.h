/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __TYPES_H__
#define __TYPES_H__

#include <stdint.h>

#define TYPE_FN_SUCCESS 0
#define TYPE_FN_FAILURE 1

#ifdef __cplusplus
extern "C" {
#endif

uint8_t type_int_eq(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_int_gt(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_int_ge(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_int_lt(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_int_le(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);

uint8_t type_ip_eq(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);

uint8_t type_str_eq(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);

uint8_t type_pubkey_eq(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);

uint8_t type_variable_eq(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_variable_gt(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_variable_ge(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_variable_lt(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_variable_le(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);

uint8_t type_varlen_eq(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_varlen_gt(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_varlen_ge(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_varlen_lt(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);
uint8_t type_varlen_le(uint8_t *lhs, uint32_t lhsize, uint8_t *rhs, uint32_t rhsize);


#ifdef __cplusplus
}
#endif

#endif /* __TYPES_H__ */
