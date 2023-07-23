/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __POL_CLUSTER_H__
#define __POL_CLUSTER_H__

#include <stdio.h>
#include "utils/list.h"

#define PC_SUCCESS 0
#define PC_FAILURE 1
#define PC_NO_CLUSTER 2
#define PC_INV_PID 3

#ifdef __cplusplus
extern "C" {
#endif

	enum {
		OP_AND = 0,
		OP_OR
	};

	enum {
		C_NON_AGGR = 0,
		C_AGGR
	};

	enum {
		PVEC_LINK = 0,
		PVEC_GROUP,
		PVEC_PROJECT,
		MAX_PVEC
	};

	typedef struct cluster {
		void *priv;
		list_t cluster_listp;
	} cluster_t;

	int init_cluster(cluster_t *cluster, int cid, int op_type, int c_type);
	int cleanup_cluster(cluster_t *cluster);
	void free_cluster(cluster_t **cluster);
	void cleanup_cluster_list(list_t *cluster_list);
	void print_cluster(cluster_t *cluster, FILE *f);
	pvec_t *cluster_get_pvec(cluster_t *cluster, int pvec_type);
	int cluster_get_id(cluster_t *cluster);
	int cluster_get_op_type(cluster_t *cluster);
	int cluster_set_pid_for_col_list(cluster_t *cluster, list_t *cs_list, 
			uint16_t pid);
	int cluster_get_pid_for_col(cluster_t *cluster, col_sym_t *cs, PVEC *g_pv);
	int16_t get_lower_bound(cluster_t *cluster, int16_t pid1, int16_t pid2);
	int cluster_set_less_restrictive_pid_list(cluster_t *cluster, int r_g_pid,
			uint16_t *pid_arr, int n_pid);
	int16_t get_least_restrictive_pid(cluster_t *cluster, PVEC pv);

#ifdef __cplusplus
}
#endif

#endif /* __POL_CLUSTER_H__ */
