/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __TUPLE_H__
#define __TUPLE_H__

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define HASH_BUF_LEN	32
#define TUP_MAX_NUM_ARGS	8

typedef uint32_t dlog_tuple_t;

typedef enum tup_type {
	TTYPE_EMPTY = 0,
	TTYPE_INTEGER,
	TTYPE_VARIABLE,
	TTYPE_TUPLE,
	TTYPE_VARLEN,
	TTYPE_UNUSED
} tup_type_t;

#define NUM_TUP_TYPES	TTYPE_UNUSED

extern uint8_t tup_type_size[NUM_TUP_TYPES];
extern const char *tup_type_name[NUM_TUP_TYPES];
uint32_t TUP_ELEMENT_START(dlog_tuple_t *tuple, int i);
uint32_t TUP_ELEMENT_START_VAR_LEN(dlog_tuple_t *tuple, int i);
void print_tuple(dlog_tuple_t *tup, char *buf);
void print_binary_to_char(char *str, int str_len, char *buf);
char *get_var_length_ptr(dlog_tuple_t *tuple, int i);
uint32_t get_var_length_ptr_len(dlog_tuple_t *tuple, int i);

#define TUP_BITMASK	(0xF)	// 4 consecutive bits set
#define TUP_TYPE_LENGTH (0x4)	// number of bytes per type

#define TUP_TYPE_SIZE(type)	tup_type_size[type]

// type of tuple element i
#define TUP_ELEMENT_TYPE(tup, i)	\
	(((tup) & (TUP_BITMASK << i * TUP_TYPE_LENGTH)) >> i * TUP_TYPE_LENGTH)

#define TUP_ELEMENT_TYPE_SET(tup, i, type)	\
	tup = ((tup & ~(TUP_BITMASK << i * TUP_TYPE_LENGTH)) |	\
				(type << i * TUP_TYPE_LENGTH))

// check if we need so many paranthesis, and double type cast to unsigned long
#define TUP_ELEMENT(tup, i)	\
	((void *) (((unsigned long) &(tup)) + sizeof(dlog_tuple_t) +	\
	 (unsigned long) TUP_ELEMENT_START(&(tup), i)))
//	((void *) (((unsigned long) (((unsigned long) &(tup)) +	\
				sizeof(dlog_tuple_t))) + (unsigned long) TUP_ELEMENT_START((&tup), i)))

#define TUP_PTR_ELEMENT(tup, i)	\
	((void *) (((unsigned long) tup) + sizeof(dlog_tuple_t) +	\
		(unsigned long) TUP_ELEMENT_START(tup, i)))

#define TUP_ELEMENT_SET(tup, i, cast, value)	\
	*((cast) (TUP_ELEMENT((tup), i))) = (value)

#define TUP_ELEMENT_TYPE_SIZE(tup, i)	\
	((TUP_ELEMENT_TYPE(tup, i) != TTYPE_TUPLE) ?	\
	 (tup_type_size[TUP_ELEMENT_TYPE(tup, i)]) :	\
	 SIZEOF_TUP(*((dlog_tuple_t *) TUP_ELEMENT(tup, i))))

#define SIZEOF_TUP(tup)	\
	((unsigned long)TUP_ELEMENT_START_VAR_LEN(&(tup), TUP_MAX_NUM_ARGS) + sizeof(dlog_tuple_t))

typedef uint8_t (*tup_eq_fn) (uint8_t *lhs, uint32_t lhsize, 
		uint8_t *rhs, uint32_t rhsize);
typedef uint8_t (*tup_gt_fn) (uint8_t *lhs, uint32_t lhsize, 
		uint8_t *rhs, uint32_t rhsize);
typedef uint8_t (*tup_ge_fn) (uint8_t *lhs, uint32_t lhsize, 
		uint8_t *rhs, uint32_t rhsize);
typedef uint8_t (*tup_lt_fn) (uint8_t *lhs, uint32_t lhsize, 
		uint8_t *rhs, uint32_t rhsize);
typedef uint8_t (*tup_le_fn) (uint8_t *lhs, uint32_t lhsize, 
		uint8_t *rhs, uint32_t rhsize);

typedef struct tup_type_if {
	tup_eq_fn eq;
	tup_gt_fn gt;
	tup_ge_fn ge;
	tup_lt_fn lt;
	tup_le_fn le;
} tup_type_if_t;

extern tup_type_if_t type_if[NUM_TUP_TYPES];

#define TUP_TYPE_EQ(type, lhs, lhsize, rhs, rhsize)	\
	type_if[type].eq(lhs, lhsize, rhs, rhsize)
#define TUP_TYPE_GT(type, lhs, lhsize, rhs, rhsize)	\
	type_if[type].gt(lhs, lhsize, rhs, rhsize)
#define TUP_TYPE_GE(type, lhs, lhsize, rhs, rhsize)	\
	type_if[type].ge(lhs, lhsize, rhs, rhsize)
#define TUP_TYPE_LT(type, lhs, lhsize, rhs, rhsize)	\
	type_if[type].lt(lhs, lhsize, rhs, rhsize)
#define TUP_TYPE_LE(type, lhs, lhsize, rhs, rhsize)	\
	type_if[type].le(lhs, lhsize, rhs, rhsize)

#define TUP_SUCCESS 0
#define TUP_FAILURE 1
#define TUP_NOT_IMPLEMENTED 2

#ifdef __cplusplus
}
#endif

#endif /* __TUPLE_H__ */
