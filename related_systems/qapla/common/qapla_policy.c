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
#include <assert.h>

#include "utils/format.h"
#include "qapla_policy.h"
#include "dlog_pi_ds.h"
#include "dlog_pi_tools.h"
#include "tuple.h"

#define QP_PERM_START_OFF(qp, perm)	\
	(qp)->perm_ptr[(perm)]

#define QP_PERM_START(qp, perm)	\
	(char *) ((char *) qp + QP_PERM_START_OFF(qp, perm))

#define QP_BASE_HDR_LEN(qp)	sizeof(qapla_policy_int_t)

#define QP_PERM_CLAUSE_HDR_OFF_TYPE(qp, perm, clause_type)	\
	(2*sizeof(uint16_t) + (clause_type==0 ? 0 : (qp)->num_perm_clauses[(perm)]*sizeof(uint16_t)))

#define QP_PERM_CLAUSE_HDR_LEN(qp, perm)	\
	((qp)->num_perm_clauses[(perm)]*sizeof(uint16_t))

#define QP_PERM_HDR_LEN(qp, perm)	\
	2*(QP_PERM_CLAUSE_HDR_LEN(qp, perm)) + 2*sizeof(uint16_t)

// serialized version of the policy
typedef struct qapla_policy_int {
	uint16_t pid;
	uint16_t perm_ptr[QP_NUM_PERMS]; // default 0, if perm not set
	uint16_t num_perm_vars[QP_NUM_PERMS];
	uint16_t num_perm_clauses[QP_NUM_PERMS];
	uint32_t last_perm_size; // includes header length
} qapla_policy_int_t;

const char *clause_name[NUM_CTYPES] = {"dlog", "sql"};
char *get_clause_type_name(uint32_t type)
{
	if (type < 0 || type >= NUM_CTYPES)
		return (char *)"";

	return (char *) clause_name[type];
}

void
qapla_init_policy(qapla_policy_t *pol, uint16_t pid)
{
	memset(pol, 0, sizeof(qapla_policy_t));
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	qp->pid = pid;
}

qapla_policy_t *
qapla_clone_policy(qapla_policy_t *pol)
{
	qapla_policy_t *qp = (qapla_policy_t *) malloc(sizeof(qapla_policy_t));
	memcpy(qp, pol, sizeof(qapla_policy_t));
	return qp;
}

void
qapla_set_perm_clauses(qapla_policy_t *pol, qapla_perm_id_t perm, uint16_t cls)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	qp->num_perm_clauses[perm] = cls;
}

uint16_t
qapla_get_num_perm_clauses(qapla_policy_t *pol, qapla_perm_id_t perm)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	return qp->num_perm_clauses[perm];
}

void
qapla_set_policy_id(qapla_policy_t *pol, uint16_t pid)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	qp->pid = pid;
}

uint16_t
qapla_get_policy_id(qapla_policy_t *pol)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	return qp->pid;
}

// ==== permissions ====
static uint16_t 
qapla_calc_next_perm_start_off(qapla_policy_int_t *qp)
{
	uint16_t off = 0;
	uint16_t max_start = 0, max_perm = QP_NUM_PERMS;
	int i;

	off += QP_BASE_HDR_LEN(qp);
	for (i = 0; i < QP_NUM_PERMS; i++) {
		if (max_start < qp->perm_ptr[i]) {
			max_start = qp->perm_ptr[i];
			max_perm = i;
		}
	}

	// no perm set
	if (max_perm == QP_NUM_PERMS) 
		return off;
	
	return max_start + qp->last_perm_size;
}

// set start offset of perm in p->perm_ptr[perm]
static void 
qapla_set_perm_start_off(qapla_policy_int_t *qp, qapla_perm_id_t perm)
{
	qp->perm_ptr[perm] = qapla_calc_next_perm_start_off(qp);
}

uint16_t
qapla_get_next_var_id(qapla_policy_t *pol, qapla_perm_id_t perm)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	return qp->num_perm_vars[perm]++;
}

uint16_t
qapla_get_num_perm_vars(qapla_policy_t *pol, qapla_perm_id_t perm)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	return qp->num_perm_vars[perm];
}

uint32_t 
qapla_policy_size(qapla_policy_t *pol)
{
	int i, last_perm = 0;
	uint32_t size = 0;

	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	//size += sizeof(qapla_policy_int_t);
	for (i = 1; i < QP_NUM_PERMS; i++) {
		if (qp->perm_ptr[i] != 0 && qp->perm_ptr[i] > qp->perm_ptr[last_perm])
			last_perm = i;
	}

	if (qp->perm_ptr[last_perm] != 0) {
		size += qp->perm_ptr[last_perm];
		size += qp->last_perm_size;
	} else {
		size += sizeof(qapla_policy_int_t);
	}

	return size;
}

char *
qapla_get_perm_start(qapla_policy_t *pol, qapla_perm_id_t perm)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	return (char *) qp + qp->perm_ptr[perm];
}

char *
qapla_start_perm(qapla_policy_t *pol, qapla_perm_id_t perm)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	qapla_set_perm_start_off(qp, perm);
	return qapla_get_perm_start(pol, perm);
}

void
qapla_end_perm(qapla_policy_t *pol, qapla_perm_id_t perm, char *end_ptr)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	char *start_ptr = qapla_get_perm_start(pol, perm);
	uint32_t perm_len = end_ptr - start_ptr;
	qp->last_perm_size = perm_len;
}

// ==== clauses in each perm ====

// offset where next sql/dlog clause of "perm" should start
// all dlog clauses consecutive, followed by all sql clauses consecutive
static uint16_t 
qapla_calc_next_perm_clause_start_off(qapla_policy_int_t *qp, 
		qapla_perm_id_t perm, int clause_type)
{
	uint16_t perm_off, perm_hdr_len;
	uint16_t perm_hdr_loc[NUM_CTYPES] = {0, 0};
	uint16_t clause_off[NUM_CTYPES] = {0, 0};
	uint16_t last_clause_len[NUM_CTYPES] = {0, 0};
	uint16_t max_start[NUM_CTYPES] = {0, 0};
	uint16_t max_cls_it[NUM_CTYPES] = {0, 0};
	uint16_t *iptr[NUM_CTYPES];
	char *ptr;
	int i;
	
	max_cls_it[CTYPE_DLOG] = max_cls_it[CTYPE_SQL] = qp->num_perm_clauses[perm];

	perm_off = qapla_calc_next_perm_start_off(qp);
	perm_hdr_len = QP_PERM_HDR_LEN(qp, perm);
	perm_hdr_loc[CTYPE_DLOG] = 
		perm_off + QP_PERM_CLAUSE_HDR_OFF_TYPE(qp, perm, CTYPE_DLOG);
	perm_hdr_loc[CTYPE_SQL] = 
		perm_off + QP_PERM_CLAUSE_HDR_OFF_TYPE(qp, perm, CTYPE_SQL); 
	ptr = (char *) qp + perm_off;
	last_clause_len[CTYPE_DLOG] = ((uint16_t *) ptr)[CTYPE_DLOG];
	last_clause_len[CTYPE_SQL] = ((uint16_t *) ptr)[CTYPE_SQL];

	ptr = (char *) qp + perm_hdr_loc[CTYPE_DLOG];
	iptr[CTYPE_DLOG] = (uint16_t *) ptr;

	for (i = 0; i < qp->num_perm_clauses[perm]; i++) {
		if (max_start[CTYPE_DLOG] < iptr[CTYPE_DLOG][i]) {
			max_start[CTYPE_DLOG] = iptr[CTYPE_DLOG][i];
			max_cls_it[CTYPE_DLOG] = i;
		}
	}

	if (max_cls_it[CTYPE_DLOG] == qp->num_perm_clauses[perm])
		clause_off[CTYPE_DLOG] = perm_off + perm_hdr_len;
	else
		clause_off[CTYPE_DLOG] = max_start[CTYPE_DLOG] + last_clause_len[CTYPE_DLOG];

	ptr = (char *) qp + perm_hdr_loc[CTYPE_SQL];
	iptr[CTYPE_SQL] = (uint16_t *) ptr;

	max_start[CTYPE_SQL] = max_start[CTYPE_DLOG];
	// continue with the max_start set above
	for (i = 0; i < qp->num_perm_clauses[perm]; i++) {
		if (max_start[CTYPE_SQL] < iptr[CTYPE_SQL][i]) {
			max_start[CTYPE_SQL] = iptr[CTYPE_SQL][i];
			max_cls_it[CTYPE_SQL] = i;
		}
	}

	if (max_cls_it[CTYPE_SQL] == qp->num_perm_clauses[perm])
		clause_off[CTYPE_SQL] = clause_off[CTYPE_DLOG]; 
	else
		clause_off[CTYPE_SQL] = max_start[CTYPE_SQL] + last_clause_len[CTYPE_SQL];

	return clause_off[clause_type];
}

// pointer to location in clause header, where start off of clause i should be set
static uint16_t *
qapla_get_perm_clause_off_loc(qapla_policy_int_t *qp, qapla_perm_id_t perm,
		int clause_type, int clause_idx)
{
	uint16_t perm_off = 0, off = 0;
	uint16_t perm_hdr_off[NUM_CTYPES] = {0, 0};
	uint16_t *iptr[NUM_CTYPES];
	char *ptr;

	perm_off = qp->perm_ptr[perm];
	perm_hdr_off[CTYPE_DLOG] =
		perm_off + QP_PERM_CLAUSE_HDR_OFF_TYPE(qp, perm, CTYPE_DLOG);
	perm_hdr_off[CTYPE_SQL] =
		perm_off + QP_PERM_CLAUSE_HDR_OFF_TYPE(qp, perm, CTYPE_SQL);

	ptr = (char *) qp + perm_hdr_off[CTYPE_DLOG];
	iptr[CTYPE_DLOG] = (uint16_t *) ptr;

	ptr = (char *) qp + perm_hdr_off[CTYPE_SQL];
	iptr[CTYPE_SQL] = (uint16_t *) ptr;

	return &(iptr[clause_type][clause_idx]);
}

static void 
qapla_set_perm_clause_start_off(qapla_policy_int_t *qp, qapla_perm_id_t perm,
		int clause_type, int clause_idx)
{
	uint16_t *coff = qapla_get_perm_clause_off_loc(qp, perm, clause_type, clause_idx);
	*coff = qapla_calc_next_perm_clause_start_off(qp, perm, clause_type);
	return;
}

static uint16_t
qapla_get_perm_clause_start_off(qapla_policy_t *pol, qapla_perm_id_t perm,
		int clause_type, int clause_idx)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	uint16_t *coff = qapla_get_perm_clause_off_loc(qp, perm, clause_type, clause_idx);
	return *coff;
}

static uint16_t
qapla_get_perm_clause_end_off(qapla_policy_t *pol, qapla_perm_id_t perm,
		int clause_type, int clause_idx)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	uint16_t perm_off = qp->perm_ptr[perm];
	char *ptr = (char *) qp + perm_off;

	uint16_t *coff = qapla_get_perm_clause_off_loc(qp, perm, clause_type, clause_idx);
	uint16_t *coff2;
	uint16_t clause_len = 0;
	int num_clauses = qp->num_perm_clauses[perm];
	int done = 0;
	int i;
	for (i = clause_idx; i < num_clauses - 1; i++) {
		coff2 = qapla_get_perm_clause_off_loc(qp, perm, clause_type, i+1);
		if (*coff2 > *coff) {
			clause_len = *coff2 - *coff;
			done = 1;
			break;
		}
	}

	if (!done)
		clause_len = ((uint16_t *) ptr)[clause_type];

	return clause_len;
}

// TODO: account for clauses where clause_type is public, and therefore not set
static char *
_qapla_get_perm_clause_start(qapla_policy_int_t *qp, qapla_perm_id_t perm,
		int clause_type, int clause_idx)
{
	uint16_t *coff = qapla_get_perm_clause_off_loc(qp, perm, clause_type, clause_idx);
	return (char *) qp + *coff;
}

// TODO: account for clauses where clause_type is public, and therefore not set
static char *
_qapla_get_perm_clause_end(qapla_policy_int_t *qp, qapla_perm_id_t perm,
		int clause_type, int clause_idx)
{
	uint16_t perm_off = qp->perm_ptr[perm];
	char *ptr = (char *) qp + perm_off;

	uint16_t *coff = qapla_get_perm_clause_off_loc(qp, perm, clause_type, clause_idx);
	uint16_t *coff2;
	uint16_t clause_len = 0;
	int num_clauses = qp->num_perm_clauses[perm];
	int done = 0;
	int i;
	for (i = clause_idx; i < num_clauses - 1; i++) {
		coff2 = qapla_get_perm_clause_off_loc(qp, perm, clause_type, i+1);
		if (*coff2 > *coff) {
			clause_len = *coff2 - *coff;
			done = 1;
			break;
		}
	}

	if (!done)
		clause_len = ((uint16_t *) ptr)[clause_type];

	char *clause_start = (char *) qp + *coff;
	char *clause_end = clause_start + clause_len;
	return clause_end;
}

char *
qapla_get_perm_clause_start(qapla_policy_t *pol, qapla_perm_id_t perm,
		int clause_type, int clause_idx)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	return _qapla_get_perm_clause_start(qp, perm, clause_type, clause_idx);
}

char *
qapla_get_perm_clause_end(qapla_policy_t *pol, qapla_perm_id_t perm,
		int clause_type, int clause_idx)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	return _qapla_get_perm_clause_end(qp, perm, clause_type, clause_idx);
}

char *
qapla_start_perm_clause(qapla_policy_t *pol, qapla_perm_id_t perm,
		int clause_type, int clause_idx)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	qapla_set_perm_clause_start_off(qp, perm, clause_type, clause_idx);
	return qapla_get_perm_clause_start(pol, perm, clause_type, clause_idx);
}

// set the size of the last clause in clause header
void
qapla_end_perm_clause(qapla_policy_t *pol, qapla_perm_id_t perm,
		int clause_type, int clause_idx, char *end_ptr)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	char *start_ptr = qapla_get_perm_clause_start(pol, perm, clause_type, clause_idx);
	uint32_t clause_len = end_ptr - start_ptr;
	uint16_t perm_off = qp->perm_ptr[perm];
	char *perm_hdr_off = (char *) qp + perm_off;
	((uint16_t *) perm_hdr_off)[clause_type] = clause_len;
}

void
qapla_unmarshall_policy(char *in, int in_len, qapla_policy_t *out)
{
	int i, j;
	qapla_perm_id_t perm = QP_PERM_READ;
	qapla_policy_int_t *in_qp = (qapla_policy_int_t *) in;
	qapla_policy_int_t *out_qp = (qapla_policy_int_t *) out->priv;

	out_qp->num_perm_clauses[perm] = in_qp->num_perm_clauses[perm];
	int num_clauses = out_qp->num_perm_clauses[perm];
	char *out_start = qapla_start_perm(out, perm);
	char *out_perm_clause_start, *out_perm_clause_end,
			 *in_perm_clause_start, *in_perm_clause_end;
	dlog_pi_op_t *out_op, *in_op;
	uint16_t *in_cls_off;

	for (j = 0; j < NUM_CTYPES; j++) {
		for (i = 0; i < num_clauses; i++) {
			in_perm_clause_start = _qapla_get_perm_clause_start(in_qp, perm, j, i);
			in_perm_clause_end = _qapla_get_perm_clause_end(in_qp, perm, j, i);
			int in_clause_len = in_perm_clause_end-in_perm_clause_start;
			in_cls_off = qapla_get_perm_clause_off_loc(in_qp, perm, j, i);
			if (in_clause_len == 0 || *in_cls_off == 0)
				continue;

			out_perm_clause_start = qapla_start_perm_clause(out, perm, j, i);
			if (j == CTYPE_DLOG) {
				memcpy(out_perm_clause_start, in_perm_clause_start, in_clause_len);
				out_perm_clause_end = out_perm_clause_start + in_clause_len;
				qapla_end_perm_clause(out, perm, j, i, (char *)out_perm_clause_end);
			} else {
				in_op = (dlog_pi_op_t *) in_perm_clause_start;
				out_op = (dlog_pi_op_t *) out_perm_clause_start;
				for (; in_op < (dlog_pi_op_t *) in_perm_clause_end;
						in_op = next_operation(in_op)) {
					uint64_t tid = *(uint64_t *) TUP_PTR_ELEMENT(&in_op->tuple, 0);
					char *sql = get_pred_sql(in_op);
					int sql_len = get_pred_sql_len(in_op);
					int tmp_len = (sql_len-1)/2;
					char *tmp = (char *) malloc(tmp_len+1);
					memset(tmp, 0, tmp_len+1);
					convert_hex_to_str(sql, sql_len, tmp);
					out_op = create_n_arg_cmd(out_op, DLOG_P_SQL, 2, TTYPE_INTEGER, &tid,
							TTYPE_VARLEN, tmp, tmp+tmp_len+1);
					free(tmp);
				}
				qapla_end_perm_clause(out, perm, j, i, (char *) out_op);
			}
		}
	}

	qapla_end_perm(out, perm, (char *) out_op);
}

// ===
uint8_t
check_allow_all_clause(qapla_policy_t *pol, qapla_perm_id_t perm,
		int clause_type, int clause_idx)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	uint16_t *coff = qapla_get_perm_clause_off_loc(qp, perm, clause_type, clause_idx);
	return (*coff == 0);
}

uint8_t
check_disallow_all_clause(qapla_policy_t *pol, qapla_perm_id_t perm,
		int clause_type, int clause_idx)
{
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	uint16_t *coff = qapla_get_perm_clause_off_loc(qp, perm, clause_type, clause_idx);
	if (*coff == 0)
		return 0;

	char *clause_start = 
		qapla_get_perm_clause_start(pol, perm, clause_type, clause_idx);
	if (clause_type == CTYPE_DLOG) {
		dlog_pi_op_t *op = (dlog_pi_op_t *) clause_start;
		if (op->cmd == DLOG_P_EMPTY)
			return 1;
	} else {
		if (strcmp(clause_start, "false") == 0)
			return 1;
	}

	return 0;
}

static void 
print_sql_clause(char *pol, char *pol_end)
{
	uint32_t cmd_cnt = 0;
	char tmp[4096];
	memset(tmp, 0, 4096);
	dlog_pi_op_t *op = (dlog_pi_op_t *) pol;
	for (; op < (dlog_pi_op_t *) pol_end; op = next_operation(op), cmd_cnt++) {
		printf("%d. %s:\n", cmd_cnt, dlog_perm_names[op->cmd]);
		print_tuple(&op->tuple, tmp);
		printf("\tArguments: %s\n", tmp);
	}
	printf("\n\n");
	//printf("len: %d, %s\n", (pol_end - pol), pol);
}

static void
print_dlog_clause(char *pol_start, char *pol_end)
{
	uint32_t cmd_cnt = 0;
	char tmp[4096];
	memset(tmp, 0, 4096);
	dlog_pi_op_t *op = (dlog_pi_op_t *) pol_start;
	for (; op < (dlog_pi_op_t *) pol_end; op = next_operation(op), cmd_cnt++) {
		printf("%d. %s:\n", cmd_cnt, dlog_perm_names[op->cmd]);
		print_tuple(&op->tuple, tmp);
		printf("\tArguments: %s\n", tmp);
	}
}

static void
print_clause(char *cls_start, char *cls_end, int cls_type)
{
	if (cls_type == CTYPE_DLOG)
		print_dlog_clause(cls_start, cls_end);
	else
		print_sql_clause(cls_start, cls_end);
}

void
print_qapla_perm(qapla_policy_t *pol, qapla_perm_id_t perm)
{
	int i, j;
	qapla_policy_int_t *qp = (qapla_policy_int_t *) pol->priv;
	int num_clauses = qp->num_perm_clauses[perm];
	int nvar = qp->num_perm_vars[perm];
	char *perm_start = qapla_get_perm_start(pol, perm);
	char *cls_start, *cls_end;
	if (qp->perm_ptr[perm] == 0) {
		printf("printing perm %d -- allow all\n", perm);
	} else {
		for (i = 0; i < num_clauses; i++) {
			for (j = 0; j < NUM_CTYPES; j++) {
				if (check_allow_all_clause(pol, perm, j, i)) {
					printf("printing perm %d, %s clause %d -- allow all\n", 
							perm, clause_name[j], i);
				} else if (check_disallow_all_clause(pol, perm, j, i)) {
					printf("printing perm %d, %s clause %d -- disallow all\n", 
							perm, clause_name[j], i);
				} else {
					cls_start = qapla_get_perm_clause_start(pol, perm, j, i);
					cls_end = qapla_get_perm_clause_end(pol, perm, j, i);
					uint16_t cls_start_off = 0, cls_end_off = 0;
					cls_start_off = qapla_get_perm_clause_start_off(pol, perm, j, i);
					cls_end_off = qapla_get_perm_clause_end_off(pol, perm, j, i);
					printf("printing perm %d, %s clause, [%u, %u], start: %p, end: %p, #vars: %d\n",
							perm, clause_name[j], cls_start_off, cls_end_off, cls_start, cls_end, 
							qp->num_perm_vars[perm]);
					print_clause(cls_start, cls_end, j);
				}
			}
		}
	}
}

void 
print_qapla_policy(qapla_policy_t *pol)
{
	if (!pol) {
		printf("p: NULL\n");
		return;
	}

	printf("Policy id: %u (%s), size: %d\n", qapla_get_policy_id(pol), pol->alias,  
			qapla_policy_size(pol));

	int i;
	uint32_t perm_it;
	for (perm_it = 0; perm_it < QP_NUM_PERMS; perm_it++) {
		print_qapla_perm(pol, (qapla_perm_id_t)perm_it);
	}
}

