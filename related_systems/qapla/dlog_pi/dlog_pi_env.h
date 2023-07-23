/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __DLOG_PI_ENV_H__
#define __DLOG_PI_ENV_H__

#include "common/qapla_policy.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_VAR_CONTENT_LEN	32

typedef struct qapla_var_int {
	uint32_t type;
	uint8_t content[MAX_VAR_CONTENT_LEN]; // for fixed length data types
	uint16_t len;
	uint8_t *content2; // for variable length data
} qapla_var_int_t;

/*
 * grow the env data structure as required
 */
typedef struct dlog_pi_env {
	uint8_t var_count;
	qapla_var_int_t *vars;
	qapla_perm_id_t perm;
	void *session;
} dlog_pi_env_t;

uint8_t dlog_pi_env_init(dlog_pi_env_t *env, qapla_policy_t *qp, 
		qapla_perm_id_t perm, void *session);
uint8_t dlog_pi_env_cleanup(dlog_pi_env_t *env);
uint8_t dlog_pi_env_setVar(dlog_pi_env_t *env, uint8_t varId, uint8_t type, 
		uint8_t *val, uint32_t val_len);
uint8_t dlog_pi_env_getVar(dlog_pi_env_t *env, uint8_t varId, uint8_t **t);
uint8_t dlog_pi_env_unsetVar(dlog_pi_env_t *env, uint8_t varId);
uint8_t dlog_pi_env_unsetAllVars(dlog_pi_env_t *env);

void dlog_pi_env_print(dlog_pi_env_t *env);

#define PI_ENV_SUCCESS 0
#define PI_ENV_OOM 1
#define PI_ENV_INVALID 2
#define PI_ENV_VAR_NOT_SET 3

#ifdef __cplusplus
}
#endif

#endif /* __DLOG_PI_ENV_H__ */
