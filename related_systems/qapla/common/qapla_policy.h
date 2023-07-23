/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QAPLA_POLICY_H__
#define __QAPLA_POLICY_H__

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
	CTYPE_DLOG = 0,
	CTYPE_SQL,
	CTYPE_UNUSED
};

#define NUM_CTYPES	CTYPE_UNUSED

#define MAX_VAR_NAME_LEN	64
#define MAX_ALIAS_LEN	64
#define MAX_POL_SIZE	(1024*16)

typedef enum qapla_perm_id {
	QP_PERM_READ = 0,
	QP_NUM_PERMS
} qapla_perm_id_t;

typedef struct qapla_policy {
	char priv[MAX_POL_SIZE];
	char alias[MAX_ALIAS_LEN];
	void *env;
} qapla_policy_t;

// == qapla policy ==
void qapla_init_policy(qapla_policy_t *qp, uint16_t pid);
qapla_policy_t *qapla_clone_policy(qapla_policy_t *pol);

void qapla_set_perm_clauses(qapla_policy_t *pol, qapla_perm_id_t perm, uint16_t cls);
uint16_t qapla_get_num_perm_clauses(qapla_policy_t *pol, qapla_perm_id_t perm);

void qapla_set_policy_id(qapla_policy_t *pol, uint16_t pid);
uint16_t qapla_get_policy_id(qapla_policy_t *pol);

uint16_t qapla_get_next_var_id(qapla_policy_t *qp, qapla_perm_id_t perm);
uint16_t qapla_get_num_perm_vars(qapla_policy_t *pol, qapla_perm_id_t perm);
uint32_t qapla_policy_size(qapla_policy_t *qp);

// pointer to start of perm
char *qapla_get_perm_start(qapla_policy_t *qp, qapla_perm_id_t perm);
// set start off of perm i in header, return pointer to start of perm i
char *qapla_start_perm(qapla_policy_t *qp, qapla_perm_id_t perm);
// set last_perm_size in qp
void qapla_end_perm(qapla_policy_t *qp, qapla_perm_id_t perm, char *end_ptr);

// return pointer to start of clause i
char *qapla_get_perm_clause_start(qapla_policy_t *qp, qapla_perm_id_t perm,
		int clause_type, int clause_idx);
// return pointer to one char after end of clause i
char *qapla_get_perm_clause_end(qapla_policy_t *qp, qapla_perm_id_t perm,
		int clause_type, int clause_idx);
// set start off of clause i in header, return pointer to start of clause i
char *qapla_start_perm_clause(qapla_policy_t *qp, qapla_perm_id_t perm,
		int clause_type, int clause_idx);
// set last clause len in clause header
void qapla_end_perm_clause(qapla_policy_t *qp, qapla_perm_id_t perm,
		int clause_type, int clause_idx, char *end_ptr);
void qapla_unmarshall_policy(char *in, int in_len, qapla_policy_t *out);

uint8_t check_allow_all_clause(qapla_policy_t *qp, qapla_perm_id_t perm,
		int clause_type, int clause_idx);
uint8_t check_disallow_all_clause(qapla_policy_t *qp, qapla_perm_id_t perm,
		int clause_type, int clause_idx);

//void query_append_policy(qapla_policy_pool_t *qpool, uint64_t polvec, int perm, 
//		char *orig_sql, char ***new_sql, int *num_sql);
void print_qapla_policy(qapla_policy_t *qp);

#ifdef __cplusplus
}
#endif

#endif /* __QAPLA_POLICY_H__ */
