/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "policyapi/sql_pred.h"
//#include "hotcrp/hotcrp_pol_db.h"
#include "common/config.h"
#include "common/qapla_policy.h"
#include "dbif/mysqlif.h"
#include "metadata.h"
#include "pol_vector.h"
#include "pol_cluster.h"
#include "query_info_int.h"
//#include "hotcrp/hotcrp_sql_pred.h"

typedef struct metadata_int {
	qapla_policy_pool_t *qpool;
	int n_cluster;
	cluster_t false_cluster;
	list_t cluster_list;
} metadata_int_t;

void
init_metadata(metadata_t *qm, db_t *schema, int db_type)
{
	if (!qm)
		return;

	metadata_int_t *qm_int = (metadata_int_t *) malloc(sizeof(metadata_int_t));
	memset(qm_int, 0, sizeof(metadata_int_t));
	qm_int->qpool = init_policy_pool(MAX_POLICIES);
	qm_int->n_cluster = 0;
	list_init(&qm_int->cluster_list);
	qm->priv = (void *) qm_int;
}

void
cleanup_metadata(metadata_t *qm)
{
	if (!qm || !qm->priv)
		return;

	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
	free_policy_pool(&qm_int->qpool);
	cleanup_cluster_list(&qm_int->cluster_list);
	free(qm_int);
	qm->priv = NULL;
}

void
free_metadata(metadata_t **qm)
{
	if (!qm || !(*qm))
		return;

	metadata_t *m = *qm;
	cleanup_metadata(m);
	free(m->priv);
	free(m);
	*qm = NULL;
}

void
print_metadata(metadata_t *qm, FILE *f)
{
	if (!qm || !f)
		return;

	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
	print_policy_pool(qm_int->qpool, f);

	cluster_t *c;
	list_t *c_head = &qm_int->cluster_list;
	list_for_each_entry(c, c_head, cluster_listp) {
		print_cluster(c, f);
	}
}

int
metadata_get_num_clusters(metadata_t *qm)
{
	if (!qm || !qm->priv) {
#if DEBUG
        printf("QM: %p\n", qm);
#endif
		return -1;
    }

	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
#if DEBUG
        printf("QM ncluster: %d\n", qm_int->n_cluster);
#endif
	return qm_int->n_cluster;
}

cluster_t *
metadata_get_false_cluster(metadata_t *qm)
{
	if (!qm || !qm->priv)
		return NULL;

	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
	return &qm_int->false_cluster;
}

list_t *
metadata_get_cluster_list(metadata_t *qm)
{
	if (!qm || !qm->priv)
		return NULL;

	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
	return &qm_int->cluster_list;
}

qapla_policy_pool_t *
metadata_get_policy_pool(metadata_t *qm)
{
	if (!qm || !qm->priv)
		return NULL;

	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
	return qm_int->qpool;
}

int
metadata_get_next_policy_cluster_id(metadata_t *qm)
{
	if (!qm || !qm->priv)
		return -1;

	int n_cluster = 0;
	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
	n_cluster = qm_int->n_cluster;
	qm_int->n_cluster++;
	return n_cluster;
}

void
metadata_add_policy_cluster(metadata_t *qm, cluster_t *c)
{
#if DEBUG
    printf("Adding policy cluster\n");
#endif
	if (!qm || !qm->priv || !c) {
#if DEBUG
        printf("Failed to add policy cluster, qm %p\n", qm);
#endif
		return;
    }

	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
	list_insert_at_tail(&qm_int->cluster_list, &c->cluster_listp);
#if DEBUG
        int nclusters = metadata_get_num_clusters(qm);
        printf("Add policy cluster %d\n", nclusters);
#endif
}

int
metadata_lookup_cluster_pvec(metadata_t *qm, col_sym_t *cs, cluster_t **cluster, 
		PVEC *pvec)
{
	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
	list_t *c_head = &qm_int->cluster_list;
	cluster_t *c;
	pvec_t *c_pv;
	PVEC tmp_pv = 0;
	int ret;
	list_for_each_entry(c, c_head, cluster_listp) {
		ret = cluster_get_pid_for_col(c, cs, &tmp_pv);
		if (!ret) {
			*cluster = c;
			*pvec = tmp_pv;
			return 0;
		}
	}
	
	return -1;
}

PVEC
get_policies_for_query(metadata_t *qm, list_t *qlist)
{
	return 0;
}

uint16_t
get_num_policies(metadata_t *qm)
{
	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
	return get_num_policy_in_pool(qm_int->qpool);
}

char *
get_pol_for_pid(metadata_t *qm, uint16_t pid)
{
	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
	qapla_policy_pool_t *qpool = qm_int->qpool;
	return get_policy_from_pool(qpool, pid);
}

list_t *
get_pol_col_for_pid(metadata_t *qm, uint16_t pid)
{
	metadata_int_t *qm_int = (metadata_int_t *) qm->priv;
	qapla_policy_pool_t *qpool = qm_int->qpool;
	return get_policy_clist_from_pool(qpool, pid);
}

void
create_metadata_table(void *conn)
{
	char buf[512];
	memset(buf, 0, 512);
	sprintf(buf,
		"CREATE TABLE %s ("
		"polId int(16) NOT NULL AUTO_INCREMENT, "
		"policy varbinary(%d) NOT NULL,"
		"PRIMARY KEY(polId)"
		") ENGINE=InnoDB DEFAULT CHARSET=utf8;", DB_POL_TABLE, MAX_POL_SIZE);

	int len = strlen(buf);
	int ret = query_mysql_conn(conn, buf, len);
	printf("create table query, ret: %d\n", ret);
}

void
drop_metadata_table(void *conn)
{
	char buf[64];
	memset(buf, 0, 64);

	sprintf(buf, "DROP TABLE %s;", DB_POL_TABLE);
	int len = strlen(buf);
	int ret = query_mysql_conn(conn, buf, len);
	printf("drop table query, ret: %d\n", ret);
}

void
add_policies_to_metadata_table(void *conn)
{
	qapla_policy_t qp_buf, *qpp;
	qpp = &qp_buf;
	char *qp = (char *) qpp;
	int pol_len = 0;
	
	int prefix_len = 0, suffix_len;
	int ret;
	
	char *buf = (char *) malloc(MAX_POL_SIZE + 64);
	memset(buf, 0, MAX_POL_SIZE + 64);
	char *pol_ptr;
	sprintf(buf,
			"INSERT INTO %s(policy) VALUES ('", DB_POL_TABLE);
	prefix_len = strlen(buf);
	pol_ptr = buf + prefix_len;
	suffix_len = 2;
	int rest = MAX_POL_SIZE + 64 - prefix_len;

#if 0
	memset(qpp, 0, sizeof(qapla_policy_t));
	g_cpfn->create_papertags_policy(qi, qp, &pol_len);
	memset(pol_ptr, 0, rest);
	memcpy(pol_ptr, qpp->priv, pol_len-1);
	sprintf(pol_ptr + (pol_len-1), "')");
	ret = query_db_conn(qi->conn, buf, prefix_len+pol_len+suffix_len);
	printf("insert policy, ret: %d\n", ret);
	//print_qapla_policy(qpp);
#endif

	free(buf);
}

void
fetch_policies_from_metadata_table(void *conn, int pid)
{
	int ret = 0;
	char buf[512];
	memset(buf, 0, 512);
	sprintf(buf, "SELECT policy from %s where polId=%d", DB_POL_TABLE, pid);

	void *res_buf = NULL;
	int nrow = 0, ncol = 0;
	ret = query_mysql_conn(conn, buf, strlen(buf));
	ret = query_mysql_result(conn, &res_buf, &nrow, &ncol);

	int i, j;
	char *pbuf;
	int pbuf_len, pbuf_type;
	qapla_policy_t qp_buf, *qpp;
	qpp = &qp_buf;
	for (i = 0; i < nrow; i++) {
		ret = result_next_row_col(res_buf, 0, &pbuf, &pbuf_len, &pbuf_type);
		memset(&qp_buf, 0, sizeof(qapla_policy_t));
		qapla_unmarshall_policy(pbuf, pbuf_len, qpp);
		//memcpy(qpp->priv, pbuf, pbuf_len);
		qapla_set_policy_id(qpp, pid);
		printf("PBUF LEN: %d, POL_SIZE: %d\n", pbuf_len, qapla_policy_size(qpp));
		print_qapla_policy(qpp);
		//add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	}
}
