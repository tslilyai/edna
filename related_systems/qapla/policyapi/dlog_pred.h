/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QAPLA_DLOG_POL_H__
#define __QAPLA_DLOG_POL_H__

#include "common/dlog_pi_tools.h"

#define create_empty(op)	create_n_arg_cmd(op, DLOG_P_EMPTY, 0)
#define create_or(op)	create_n_arg_cmd(op, DLOG_P_OR, 0)

#define create_session_is(op, ...)	create_n_arg_cmd(op, DLOG_P_SESSID, 1, __VA_ARGS__)
#define create_time_is(op, ...)	create_n_arg_cmd(op, DLOG_P_TIME, 1, __VA_ARGS__)

#define create_gt(op, ...)	create_n_arg_cmd(op, DLOG_P_GT, 2, __VA_ARGS__)
#define create_ge(op, ...)	create_n_arg_cmd(op, DLOG_P_GE, 2, __VA_ARGS__)
#define create_lt(op, ...)	create_n_arg_cmd(op, DLOG_P_LT, 2, __VA_ARGS__)
#define create_le(op, ...)	create_n_arg_cmd(op, DLOG_P_LE, 2, __VA_ARGS__)
#define create_eq(op, ...)	create_n_arg_cmd(op, DLOG_P_EQ, 2, __VA_ARGS__)
#define create_ne(op, ...)	create_n_arg_cmd(op, DLOG_P_NE, 2, __VA_ARGS__)
#define create_sql(op, ...)	create_n_arg_cmd(op, DLOG_P_SQL, 2, __VA_ARGS__)

#endif /* __QAPLA_DLOG_POL_H__ */
