/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __DLOG_PI_HELPER_H__
#define __DLOG_PI_HELPER_H__

#include "dlog_pi_env.h"
#include "common/tuple.h"

#ifdef __cplusplus
extern "C" {
#endif

tup_type_t lookup_elem_type(dlog_pi_env_t *env, dlog_tuple_t *tup, uint8_t id);
uint8_t *lookup_elem_value(dlog_pi_env_t *env, dlog_tuple_t *tup, uint8_t id);
uint8_t *lookup_var_len_ptr(dlog_pi_env_t *env, dlog_tuple_t *tup, uint8_t id);
uint32_t lookup_elem_size(dlog_pi_env_t *env, dlog_tuple_t *tup, uint8_t id);
uint8_t compare_or_set(dlog_pi_env_t *env, dlog_tuple_t *tup, uint8_t id,
		tup_type_t type, void *env_content, int env_cont_len);

#ifdef __cplusplus
}
#endif

#endif /* __DLOG_PI_HELPER_H__ */
