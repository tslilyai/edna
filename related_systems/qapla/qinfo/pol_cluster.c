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
#include <stdint.h>
#include <string.h>

#include "common/config.h"
#include "qinfo/policy_pool.h"
#include "pol_vector.h"
#include "pol_cluster.h"

#define CLUSTER_PVEC_SIZE	20

#define CLUSTER_PID(c, g_pid)	\
	(g_pid < MAX_POLICIES) ? c->global_pid_map[g_pid] : -1

#define GLOBAL_PID(c, c_pid)	\
	(c_pid < MAX_POLICIES) ? c->cluster_pid_map[c_pid] : -1

typedef struct cluster_int {
	int8_t cluster_id;
	uint8_t cluster_op;
	uint8_t cluster_type; // aggregate or non-aggregate policies
	list_t pid_order[MAX_POLICIES]; // less restrictive pids, indexed by cluster pid
	pvec_t cluster_pvec[MAX_PVEC]; // reverse index on cluster policies
} cluster_int_t;

typedef struct pid_node {
	uint16_t pid;
	list_t pid_orderp;
} pid_node_t;

pid_node_t *
alloc_init_pid_node(uint16_t pid)
{
	pid_node_t *pn = (pid_node_t *) malloc(sizeof(pid_node_t));
	pn->pid = pid;
	list_init(&pn->pid_orderp);
	return pn;
}

void
free_pid_node(pid_node_t **pn)
{
	if (!pn || !(*pn))
		return;

	pid_node_t *node = *pn;
	list_init(&node->pid_orderp);
	free(node);
	*pn = NULL;
}

int16_t
get_lower_bound(cluster_t *c, int16_t pid1, int16_t pid2)
{
	if (pid2 < 0)
		return pid1;
	if (pid1 < 0)
		return pid2;
	if (pid1 == pid2)
		return pid1;

	cluster_int_t *c_int = (cluster_int_t *) c->priv;

	int16_t c_pid1, c_pid2;
	list_t *order_head1, *order_head2;
	pid_node_t *pn1, *pn2;
	
	c_pid1 = pid1;
	c_pid2 = pid2;

	order_head1 = &c_int->pid_order[c_pid1];
	list_for_each_entry(pn1, order_head1, pid_orderp) {
		if (pn1->pid == c_pid2)
			return pid2;
	}
	
	order_head2 = &c_int->pid_order[c_pid2];
	list_for_each_entry(pn2, order_head2, pid_orderp) {
		if (pn2->pid == c_pid1)
			return pid1;
	}

	order_head1 = &c_int->pid_order[c_pid1];
	order_head2 = &c_int->pid_order[c_pid2];
	list_for_each_entry(pn1, order_head1, pid_orderp) {
		list_for_each_entry(pn2, order_head2, pid_orderp) {
			if (pn1->pid == pn2->pid) {
				return pn1->pid;
			}
		}
	}

	// we shouldn't really reach here
	return -1;
}

int
init_cluster(cluster_t *cluster, int cid, int op_type, int c_type)
{
	if (!cluster)
		return PC_NO_CLUSTER;

	int i, num_pvec;
	cluster_int_t *c_int = (cluster_int_t *) malloc(sizeof(cluster_int_t));
	memset(c_int, 0, sizeof(cluster_int_t));
	c_int->cluster_id = cid;
	c_int->cluster_op = op_type;
	c_int->cluster_type = c_type;
	for (i = 0; i < MAX_POLICIES; i++) {
		list_init(&c_int->pid_order[i]);
	}

	num_pvec = (c_type == C_NON_AGGR) ? 1 : MAX_PVEC;
	for (i = 0; i < num_pvec; i++) {
		init_pvec_index(&c_int->cluster_pvec[i], &ht_colid_pvec, CLUSTER_PVEC_SIZE);
	}

	cluster->priv = (void *) c_int;
	list_init(&cluster->cluster_listp);

	return PC_SUCCESS;
}

int
cleanup_cluster(cluster_t *cluster)
{
	if (!cluster)
		return PC_NO_CLUSTER;

	int i, num_pvec;
	cluster_int_t *c_int = (cluster_int_t *) cluster->priv;
	for (i = 0; i < MAX_POLICIES; i++) {
		list_t *head = &c_int->pid_order[i];
		list_t *it = head->next;
		list_t *next;
		while (it != head) {
			next = it->next;
			pid_node_t *pn = list_entry(it, pid_node_t, pid_orderp);
			list_remove(it);
			free(pn);
			it = next;
		}
		list_init(&c_int->pid_order[i]);
	}

	num_pvec = (c_int->cluster_type == C_NON_AGGR) ? 1 : MAX_PVEC;
	for (i = 0; i < num_pvec; i++) {
		cleanup_pvec_index(&c_int->cluster_pvec[i]);
	}
	free(c_int);

	cluster->priv = NULL;
	list_init(&cluster->cluster_listp);
	return PC_SUCCESS;
}

void
free_cluster(cluster_t **c_pp)
{
	if (!c_pp)
		return;

	cluster_t *c_p = *c_pp;
	if (!c_p)
		return;

	cleanup_cluster(c_p);
	free(c_p);
	*c_pp = NULL;
}

void
cleanup_cluster_list(list_t *cluster_list)
{
	list_t *cls_head = cluster_list;
	list_t *cls_it = cls_head->next;
	list_t *cls_next;
	cluster_t *c;
	while (cls_it != cls_head) {
		cls_next = cls_it->next;
		c = list_entry(cls_it, cluster_t, cluster_listp);
		list_remove(cls_it);
		free_cluster(&c);
		cls_it = cls_next;
	}
}

void
print_cluster(cluster_t *cluster, FILE *f)
{
	if (!cluster)
		return;

	cluster_int_t *c_int = (cluster_int_t *) cluster->priv;
	fprintf(f, "-- CLUSTER #%d (type: %d, op: %d) --\n", c_int->cluster_id,
			c_int->cluster_type, c_int->cluster_op);
	int i, num_pvec;
	pvec_t *pv;
	num_pvec = (c_int->cluster_type == C_NON_AGGR) ? 1 : MAX_PVEC;
	for (i = 0; i < num_pvec; i++) {
		pv = &c_int->cluster_pvec[i];
		print_pvec_index(pv, f);
	}
}

pvec_t *
cluster_get_pvec(cluster_t *cluster, int pvec_type)
{
	if (!cluster)
		return NULL;

	if (pvec_type < 0 || pvec_type >= MAX_PVEC)
		return NULL;

	cluster_int_t *c_int = (cluster_int_t *) cluster->priv;
	return &c_int->cluster_pvec[pvec_type];
}

int
cluster_get_id(cluster_t *cluster)
{
	if (!cluster)
		return -1;

	cluster_int_t *c_int = (cluster_int_t *) cluster->priv;
	return c_int->cluster_id;
}

int
cluster_get_op_type(cluster_t *cluster)
{
	if (!cluster)
		return -1;

	cluster_int_t *c_int = (cluster_int_t *) cluster->priv;
	return c_int->cluster_op;
}

int
cluster_set_pid_for_col_list(cluster_t *cluster, list_t *cs_list, uint16_t c_pid)
{
	if (!cluster)
		return PC_NO_CLUSTER;

	int i;
	int expr_type, expr_type_bit;
	col_sym_t *cs;
	list_t *cs_it = cs_list;
	cluster_int_t *c_int = (cluster_int_t *) cluster->priv;
	pvec_t *link_pv = &c_int->cluster_pvec[PVEC_LINK];
	pvec_t *group_pv = &c_int->cluster_pvec[PVEC_GROUP];
	pvec_t *project_pv = &c_int->cluster_pvec[PVEC_PROJECT];
	
	if (c_int->cluster_type == C_NON_AGGR) {
		group_pv = link_pv;
		project_pv = link_pv;
	}

	list_for_each_entry(cs, cs_it, col_sym_listp) {
		expr_type = sym_field(cs, type);
		expr_type_bit = expr_type & (JOIN_COND|FILTER);
		if (expr_type_bit)
			add_col_policy_to_pvec_index(link_pv, c_pid, cs);
		expr_type_bit = expr_type & (GROUP|HAVING);
		if (expr_type_bit)
			add_col_policy_to_pvec_index(group_pv, c_pid, cs);
		expr_type_bit = expr_type & PROJECT;
		if (expr_type_bit)
			add_col_policy_to_pvec_index(project_pv, c_pid, cs);
	}
	// TODO: traverse columns here and add policy in appropriate pvec
	//add_policy_to_pvec_index(&c_int->cluster_pvec, c_pid, cs_list);
	return PC_SUCCESS;
}

int
cluster_get_pid_for_col(cluster_t *cluster, col_sym_t *cs, PVEC *g_pv)
{
	if (!cluster)
		return PC_NO_CLUSTER;

	cluster_int_t *c_int = (cluster_int_t *) cluster->priv;
	PVEC cluster_pvec = -1;

	int expr_type;
	pvec_t *link_pv = &c_int->cluster_pvec[PVEC_LINK];
	pvec_t *group_pv = &c_int->cluster_pvec[PVEC_GROUP];
	pvec_t *project_pv = &c_int->cluster_pvec[PVEC_PROJECT];

	if (c_int->cluster_type == C_NON_AGGR) {
		group_pv = link_pv;
		project_pv = link_pv;
	}

	expr_type = sym_field(cs, type);
	switch (expr_type) {
		case JOIN_COND:
		case FILTER:
		case ORDER:
			cluster_pvec = get_column_pvec(link_pv, cs);
			break;

		case GROUP:
		case HAVING:
			cluster_pvec = get_column_pvec(group_pv, cs);
			break;

		case PROJECT:
			cluster_pvec = get_column_pvec(project_pv, cs);
			break;
	}

	// TODO: go through each col_sym, and lookup appropriate pvec type
	//PVEC cluster_pvec = get_policy_from_pvec_index(&c_int->cluster_pvec, cs_list);
	if ((int64_t)cluster_pvec < 0)
		return PC_FAILURE;
	
	*g_pv |= cluster_pvec;

	return PC_SUCCESS;
}

// for each pid in pid_arr, pid less restrictive than r_pid 
int
cluster_set_less_restrictive_pid_list(cluster_t *cluster, int r_pid, 
		uint16_t *pid_arr, int n_pid)
{
	if (!cluster)
		return PC_NO_CLUSTER;

	cluster_int_t *c_int = (cluster_int_t *) cluster->priv;
	list_t *order_head = &c_int->pid_order[r_pid];
	pid_node_t *pn;

	int i;
	for (i = 0; i < n_pid; i++) {
		uint16_t g_pid = pid_arr[i];
		pn = alloc_init_pid_node(g_pid);
		list_insert(order_head, &pn->pid_orderp);
	}

	return PC_SUCCESS;
}

int16_t
get_least_restrictive_pid(cluster_t *cluster, PVEC pv)
{
	int pid_it, is_set;
	int16_t tmp_pid, least_pid = -1;
	PVEC pid_bit = 0;

	for (pid_it = 0; pid_it < MAX_POLICIES; pid_it++) {
		pid_bit = (PVEC) (pv >> pid_it);
		is_set = (int) (pid_bit & 1);
		if (is_set == 0)
			continue;

		tmp_pid = (int16_t) pid_it;
		least_pid = get_lower_bound(cluster, tmp_pid, least_pid);
	}

	return least_pid;
}
