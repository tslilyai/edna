/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __DLOG_PI_DS_H__
#define __DLOG_PI_DS_H__

#include "tuple.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum dlog_pol_cmds {
	DLOG_P_EMPTY = 0,
	DLOG_P_OR,
	DLOG_P_SESSID,
	DLOG_P_TIME,
	DLOG_P_GT,
	DLOG_P_GE,
	DLOG_P_LT,
	DLOG_P_LE,
	DLOG_P_EQ,
	DLOG_P_NE,
	DLOG_P_SQL,
	DLOG_P_LAST_CMD
} dlog_pol_cmd_t;

#define NUM_DLOG_P_CMDS	DLOG_P_LAST_CMD

extern const char *dlog_perm_names[NUM_DLOG_P_CMDS];

typedef struct dlog_pi_op {
	dlog_pol_cmd_t cmd;
	dlog_tuple_t tuple;
} dlog_pi_op_t;

#ifdef __cplusplus
}
#endif

#endif /* __DLOG_PI_DS_H__ */
