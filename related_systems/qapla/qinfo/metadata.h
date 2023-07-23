/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __POL_DB_UTILS_H__
#define __POL_DB_UTILS_H__

#include "utils/list.h"
#include "common/db.h"
#include "common/col_sym.h"
#include "policy_pool.h"
#include "pol_vector.h"
#include "pol_cluster.h"

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct metadata {
		void *priv;
	} metadata_t;

	typedef struct app_info {
		void *sql_pred_fns;
		void *pol_fns;
		void *col_fns;
		int (*load_schema_fn) (db_t *db);
		void (*load_policies_fn) (db_t *db, metadata_t *qm,
				void *sql_pred_fns, void *pol_fns, void *col_fns);
	} app_info_t;

	void init_metadata(metadata_t *qm, db_t *schema, int db_type);
	void cleanup_metadata(metadata_t *qm);
	void free_metadata(metadata_t **qm);
	void print_metadata(metadata_t *qm, FILE *f);
	int metadata_get_num_clusters(metadata_t *qm);
	cluster_t *metadata_get_false_cluster(metadata_t *qm);
	list_t *metadata_get_cluster_list(metadata_t *qm);
	qapla_policy_pool_t *metadata_get_policy_pool(metadata_t *qm);
	int metadata_get_next_policy_cluster_id(metadata_t *qm);
	void metadata_add_policy_cluster(metadata_t *qm, cluster_t *c);
	int metadata_lookup_cluster_pvec(metadata_t *qm, col_sym_t *cs, cluster_t **c,
			PVEC *pvec);
	PVEC get_policies_for_query(metadata_t *qm, list_t *qlist);

	uint16_t get_num_policies(metadata_t *qm);
	char *get_pol_for_pid(metadata_t *qm, uint16_t pid);
	list_t *get_pol_col_for_pid(metadata_t *qm, uint16_t pid);

	void create_metadata_table(void *conn);
	void drop_metadata_table(void *conn);
	void add_policies_to_metadata_table(void *conn);
	void fetch_policies_from_metadata_table(void *conn, int pid);
#ifdef __cplusplus
}
#endif
#endif /* __POL_DB_UTILS_H__ */
