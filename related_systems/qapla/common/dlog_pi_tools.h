/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __DLOG_PI_TOOLS_H__
#define __DLOG_PI_TOOLS_H__

#include "dlog_pi_ds.h"
#include "tuple.h"

#ifdef __cplusplus
extern "C" {
#endif
	dlog_pi_op_t *create_n_arg_cmd(dlog_pi_op_t *op, dlog_pol_cmd_t cmd, 
			uint32_t num_args,...);
	dlog_pi_op_t *next_operation(dlog_pi_op_t *op);
	char *get_pred_sql(dlog_pi_op_t *op);
	int get_pred_sql_len(dlog_pi_op_t *op);

#ifdef __cplusplus
}
#endif

#endif /* __DLOG_PI_TOOLS_H__ */
