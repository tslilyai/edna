#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "utils/time.h"
#include "utils/list.h"
#include "common/qapla_policy.h"
#include "common/dlog_pi_ds.h"
#include "common/col_sym.h"
#include "common/db.h"
#include "hotcrp_pol_db.h"
#include "policyapi/dlog_pred.h"
#include "policyapi/sql_pred.h"
#include "hotcrp_sql_pred.h"

uint64_t conf_time = get_timestamp((char *)"2016-05-31 08:00:00");
uint64_t time_sub = get_timestamp((char *) "2016-05-26 08:00:00");
uint64_t time_rev_start = get_timestamp((char *) "2016-05-28 08:00:00");
uint64_t time_rev_discuss = get_timestamp((char *) "2016-05-29 08:00:00");
uint64_t time_notify = get_timestamp((char *) "2016-05-30 08:00:00");

/*
 * false policy: only need to set false in datalog part
 */
void 
_qapla_create_sql_false_policy(db_t *schema, char *tname, char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	int op_it = 0;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);

	char *false_str = (char *) "1=0";
	uint64_t tid = get_schema_table_id(schema, tname);

	char sql_buf[64], *sql_buf_end;
	memset(sql_buf, 0, 64);

	// == sql ==
	sprintf(sql_buf, "%s", false_str);
	sql_buf_end = sql_buf + strlen(false_str) + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);
	return;
}

static char *
_qapla_create_sql_true_op(db_t *schema, sql_pred_t *g_sql_pred,
		dlog_pi_op_t *op, char *tname)
{
	dlog_pi_op_t *sql_op = op;
	uint64_t tid = get_schema_table_id(schema, tname);

	char *true_str = (char *) "1=1";
	int true_str_len = strlen(true_str);
	char sql_buf[64], *sql_buf_end;
	memset(sql_buf, 0, 64);

	sprintf(sql_buf, "%s", true_str);
	sql_buf_end = sql_buf + true_str_len + 1;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid,
			TTYPE_VARLEN, sql_buf, sql_buf_end);

	return (char *) sql_op;
}

void
_qapla_create_sql_true_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *tname, char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	int op_it = 0;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);

	// == sql ==
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_true_op(schema, g_sql_pred,
			sql_op, tname);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);
}

static char *
_qapla_create_sql_full_contact_info(db_t *schema, sql_pred_t *g_sql_pred,
		dlog_pi_op_t *op, uint16_t sess_var)
{
	int connector = 0;
	uint64_t role = 0;
	char *tab_contact = (char *) "ContactInfo";
	uint64_t tid_contact = get_schema_table_id(schema, tab_contact);
	char *pred[2];
	char *tmp_sql_buf[4];
	char *sql_buf, *sql_buf_end;
	int sql_buf_len = 2048;
	int i, pred_len = 0;
	uint64_t conflict_type;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}
	for (i = 0; i < 4; i++) {
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	sql_buf = (char *) malloc(sql_buf_len);
	memset(sql_buf, 0, sql_buf_len);

	role = ROLE_CHAIR | ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	combine_pred(tmp_sql_buf[0], &pred_len, pred, 1, 0);

	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 1024);

	role = ROLE_CHAIR|ROLE_PC;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	role = ROLE_CHAIR|ROLE_PC|ROLE_REVIEWER;
	g_sql_pred->create_data_role_pred(pred[1], 1, TTYPE_INTEGER, &role);
	combine_pred(tmp_sql_buf[1], &pred_len, pred, 2, 0);

	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 1024);

	g_sql_pred->create_data_contact_email_pred(pred[0], 1, TTYPE_VARIABLE, &sess_var);
	combine_pred(tmp_sql_buf[2], &pred_len, pred, 1, 0);

	conflict_type = CONFLICT_AUTHOR;
	g_sql_pred->create_contact_conflict_pred(tmp_sql_buf[3], 2, 
			TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &conflict_type);

	connector |= 1;
	connector |= (1 << 1);
	connector |= (1 << 2);
	combine_pred(sql_buf, &pred_len, tmp_sql_buf, 4, connector);
	sql_buf_end = sql_buf + pred_len + 1;

	op = create_n_arg_cmd(op, DLOG_P_SQL, 2, TTYPE_INTEGER, &tid_contact, 
			TTYPE_VARLEN, sql_buf, sql_buf_end);

	for (i = 0; i < 2; i++)
		free(pred[i]);
	for (i = 0; i < 4; i++)
		free(tmp_sql_buf[i]);
	free(sql_buf);

	return (char *) op;
}

/*
 * C1: 
 * ContactInfo.{firstName, lastName, affiliation, roles}
 */
void 
qapla_create_contact_info_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 4;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "contact_info");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	
	// == dlog 1 ==
	// session_is($k)
	op_it = 0;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 2 == 
	// public
	op_it++;
	
	// == dlog 3 ==
	// curr_time >= T_CONF
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_gt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &conf_time);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 4 ==
	// curr_time >= T_NOTIFY and session_is($k)
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_gt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_notify);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	char *tab_contact = (char *) "ContactInfo";
	uint64_t tid_contact = (uint64_t) get_schema_table_id(schema, tab_contact);
	uint64_t role = ROLE_CHAIR | ROLE_ADMIN;
	int pred_len = 0;
	char *pred[4];
	char *tmp_sql_buf[2];
	char sql_buf[2048], *sql_buf_end;
	uint64_t conflict_type;
	int connector = 0;
	int i;
	for (i = 0; i < 4; i++)
		pred[i] = (char *) malloc(1024);

	for (i = 0; i < 2; i++) {
		tmp_sql_buf[i] = (char *) malloc(512);
		memset(tmp_sql_buf[i],0, 512);
	}

	// == sql 1 ==
	// P_ROLE(CHAIR | ROLE_ADMIN) or 
	// (ContactInfo.email=$k) or co-authors of user's paper
	// or (P_ROLE(PC) and ContactInfo.role=CHAIR|PC|REVIEWER)
	memset(sql_buf, 0, 2048);
	for (i = 0; i < 4; i++)
		memset(pred[i], 0, 1024);

	op_it = 0;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var); 
	g_sql_pred->create_data_contact_email_pred(pred[1], 1, TTYPE_VARIABLE, &sess_var);

	conflict_type = CONFLICT_AUTHOR;
	g_sql_pred->create_contact_conflict_pred(pred[2], 2, TTYPE_VARIABLE,
			&sess_var, TTYPE_INTEGER, &conflict_type);

	role = ROLE_PC;
	g_sql_pred->create_user_role_email_pred(tmp_sql_buf[0], 2,
			TTYPE_INTEGER, &role, TTYPE_VARIABLE, &sess_var);
	role = ROLE_CHAIR|ROLE_PC|ROLE_REVIEWER;
	g_sql_pred->create_data_role_pred(tmp_sql_buf[1], 1,
			TTYPE_INTEGER, &role);
	combine_pred(pred[3], &pred_len, tmp_sql_buf, 2, 0);

	connector = 1 | (1 << 1) | (1 << 2);
	combine_pred(sql_buf, &pred_len, pred, 4, connector);
	sql_buf_end = sql_buf + pred_len + 1;

	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_contact,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// ContactInfo.role=CHAIR|PC
	role = ROLE_CHAIR|ROLE_PC;
	memset(sql_buf, 0, 2048);
	for (i = 0; i < 4; i++)
		memset(pred[i], 0, 1024);

	op_it++;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;

	g_sql_pred->create_data_role_pred(pred[0], 1, TTYPE_INTEGER, &role);
	combine_pred(sql_buf, &pred_len, pred, 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_contact,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 3 ==
	// ContactInfo.role=REVIEWER
	role = ROLE_REVIEWER;
	memset(sql_buf, 0, 2048);
	for (i = 0; i < 4; i++)
		memset(pred[i], 0, 1024);

	op_it++;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;

	g_sql_pred->create_data_role_pred(pred[0], 1, TTYPE_INTEGER, &role);
	combine_pred(sql_buf, &pred_len, pred, 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_contact,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 4 ==
	// P_ROLE(PC) and contact is author of an accepted paper
	role = ROLE_CHAIR | ROLE_PC;
	memset(sql_buf, 0, 2048);
	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 1024);

	op_it++;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_accepted_paper_auth_pred(pred[1], 0);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_contact,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 4; i++)
		free(pred[i]);
	for (i = 0; i < 2; i++)
		free(tmp_sql_buf[i]);

	return;
}

/*
 * C2: 
 * ContactInfo.{email, preferredEmail}
 */
void 
qapla_create_contact_email_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "contact_email");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	// == dlog 1 ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 2 ==
	// curr_time >= T_NOTIFY and session_is($k)
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_gt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_notify);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);
	
	uint64_t role;
	char *tab_contact = (char *) "ContactInfo";
	uint64_t tid_contact = get_schema_table_id(schema, tab_contact);
	char sql_buf[2048], *sql_buf_end;
	char *pred[2];
	char *tmp_sql_buf[2];
	int pred_len;

	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	memset(sql_buf, 0, 2048);

	// == sql 1 ==
	// P_ROLE(CHAIR | ROLE_ADMIN) or 
	// (P_ROLE(CHAIR|PC) and ContactInfo.role=CHAIR|PC|REVIEWER) or
	// ContactInfo.email=$k or email of co-author of user's paper
	op_it = 0;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_full_contact_info(schema, g_sql_pred,
			sql_op, sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// P_CONTACT_PAPER_SHEPH or
	// P_ROLE(PC) and contact is author of an accepted paper
	memset(sql_buf, 0, 2048);
	role = ROLE_CHAIR|ROLE_PC;
	op_it++;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	g_sql_pred->create_contact_paper_sheph_pred(tmp_sql_buf[0], 
			2, TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role);

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER,
			&role, TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_accepted_paper_auth_pred(pred[1], 0);
	combine_pred(tmp_sql_buf[1], &pred_len, pred, 2, 0);

	combine_pred(sql_buf, &pred_len, tmp_sql_buf, 2, 1);
	sql_buf_end = sql_buf + pred_len + 1;

	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_contact,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);
	for (i = 0; i < 2; i++)
		free(tmp_sql_buf[i]);

	return;
}

/*
 * C3: ContactInfo.{contactId}
 */
void
qapla_create_contact_id_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "contact_id");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	// == dlog 1 ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 2 ==
	// curr_time >= T_NOTIFY and session_is($k)
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_gt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_notify);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	uint64_t role;
	char *tab_contact = (char *) "ContactInfo";
	uint64_t tid_contact = get_schema_table_id(schema, tab_contact);
	char sql_buf[2048], *sql_buf_end;
	char *pred[4];
	int pred_len;
	int connector = 0;
	uint64_t conflict_type;

	int i;
	for (i = 0; i < 4; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}
	memset(sql_buf, 0, 2048);

	// == sql 1 ==
	// P_ROLE(CHAIR|PC|ADMIN) or ContactInfo.email=$k or (ContactInfo.roles=CHAIR|PC)
	op_it = 0;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);

	g_sql_pred->create_data_contact_email_pred(pred[1], 1, TTYPE_VARIABLE, &sess_var);

	role = ROLE_CHAIR|ROLE_PC;
	g_sql_pred->create_data_role_pred(pred[2], 1, TTYPE_INTEGER, &role);
	
	conflict_type = CONFLICT_AUTHOR;
	g_sql_pred->create_contact_conflict_pred(pred[3], 2, TTYPE_VARIABLE,
			&sess_var, TTYPE_INTEGER, &conflict_type);

	connector |= 1;
	connector |= (1 << 1);
	connector |= (1 << 2);
	combine_pred(sql_buf, &pred_len, pred, 4, connector);
	sql_buf_end = sql_buf + pred_len + 1;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_contact,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// P_ROLE(PC) and contact is author of an accepted paper
	role = ROLE_CHAIR | ROLE_PC;
	memset(sql_buf, 0, 2048);
	for (i = 0; i < 4; i++)
		memset(pred[i], 0, 1024);

	op_it++;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_accepted_paper_auth_pred(pred[1], 0);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_contact,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 4; i++)
		free(pred[i]);

	return;
}

void
qapla_create_contact_pc_view_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "pc_view_contact_info");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// ContactInfo.email=$k or P_ROLE(CHAIR|ADMIN) or 
	// (P_ROLE(PC) and ContactInfo.role=CHAIR|PC|REVIEWER)
	uint64_t role;
	char *tab_contact = (char *) "ContactInfo";
	uint64_t tid_contact = get_schema_table_id(schema, tab_contact);
	char *pred[2];
	char *tmp_sql_buf[3];
	char sql_buf[512], *sql_buf_end;
	int pred_len;
	int connector = 0;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	for (i = 0; i < 3; i++) {
		tmp_sql_buf[i] = (char *) malloc(512);
		memset(tmp_sql_buf[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	role = ROLE_PC;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	role = ROLE_CHAIR|ROLE_PC|ROLE_REVIEWER;
	g_sql_pred->create_data_role_pred(pred[1], 1, TTYPE_INTEGER, &role);
	combine_pred(tmp_sql_buf[1], &pred_len, pred, 2, 0);

	g_sql_pred->create_data_contact_email_pred(tmp_sql_buf[0], 1, 
			TTYPE_VARIABLE, &sess_var);

	role = ROLE_CHAIR|ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(tmp_sql_buf[2], 2, 
			TTYPE_INTEGER, &role, TTYPE_VARIABLE, &sess_var);

	connector = 1 | (1 << 1);
	combine_pred(sql_buf, &pred_len, tmp_sql_buf, 3, connector);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_contact,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++) {
		free(pred[i]);
	}
	for (i = 0; i < 3; i++) {
		free(tmp_sql_buf[i]);
	}

	return;
}

void
qapla_create_contact_private_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "private_contact_info");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// ContactInfo.email=$k
	char *tab_contact = (char *) "ContactInfo";
	uint64_t tid_contact = get_schema_table_id(schema, tab_contact);
	char *pred[1];
	char sql_buf[512], *sql_buf_end;
	int pred_len;
	int i;
	for (i = 0; i < 1; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	g_sql_pred->create_data_contact_email_pred(pred[0], 1, TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_contact,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 1; i++)
		free(pred[i]);

	return;
}

void
qapla_create_contact_chair_owner_view_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "chair_owner_view_ci");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// ContactInfo.email=$k or P_ROLE(CHAIR|ADMIN)
	uint64_t role = ROLE_CHAIR|ROLE_ADMIN;
	char *tab_contact = (char *) "ContactInfo";
	uint64_t tid_contact = get_schema_table_id(schema, tab_contact);
	char *pred[2];
	char sql_buf[512], *sql_buf_end;
	int pred_len;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	g_sql_pred->create_data_contact_email_pred(pred[0], 1, TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_user_role_email_pred(pred[1], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 2, 1);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_contact,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);

	return;
}

char *
_qapla_create_sql_user_paper_author(db_t *schema, sql_pred_t *g_sql_pred,
		dlog_pi_op_t *op, uint16_t sess_var, char **tab_list, int num_tables)
{
	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;

	uint64_t *tid;
	char *pred[2];
	char sql_buf[2048], *sql_buf_end;
	int pred_len = 0;
	uint64_t role;

	tid = (uint64_t *) malloc(sizeof(uint64_t) * num_tables);
	memset(tid, 0, sizeof(uint64_t) * num_tables);

	int i;
	for (i = 0; i < num_tables; i++) {
		tid[i] = get_schema_table_id(schema, tab_list[i]);
	}

	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}

	sql_op = op;
	for (i = 0; i < num_tables; i++) {
		g_sql_pred->create_table_paper_author_pred(pred[0], 2, 
				TTYPE_VARLEN, tab_list[i], tab_list[i] + strlen(tab_list[i]) + 1,
				TTYPE_VARIABLE, &sess_var);
		role = ROLE_CHAIR|ROLE_ADMIN;
		g_sql_pred->create_user_role_email_pred(pred[1], 2, TTYPE_INTEGER, &role,
				TTYPE_VARIABLE, &sess_var);
		memset(sql_buf, 0, 2048);
		combine_pred(sql_buf, &pred_len, pred, 2, 1);
		sql_buf_end = sql_buf + pred_len + 1;
		sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid[i],
				TTYPE_VARLEN, sql_buf, sql_buf_end);
	}

	free(tid);
	for (i = 0; i < 2; i++) {
		free(pred[i]);
	}

	return (char *) sql_op;
}

char *
_qapla_create_sql_review_phase_op(db_t *schema, sql_pred_t *g_sql_pred,
		dlog_pi_op_t *op, uint16_t sess_var, char **tab_list, int num_tables)
{
	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;

	uint64_t *tid = (uint64_t *) malloc(sizeof(uint64_t) * num_tables);
	memset(tid, 0, sizeof(uint64_t) * num_tables);
	int tab_it;
	for (tab_it = 0; tab_it < num_tables; tab_it++) {
		tid[tab_it] = get_schema_table_id(schema, tab_list[tab_it]);
	}

	char *tab_paper = (char *) "Paper";
	uint64_t role, conflict;
	char *pred[3];
	char * tmp_sql_buf[4];
	char sql_buf[2048], *sql_buf_end;
	int pred_len;
	int i;
	for (i = 0; i < 3; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}
	for (i = 0; i < 4; i++) {
		tmp_sql_buf[i] = (char *) malloc(512);
		memset(tmp_sql_buf[i], 0, 512);
	}
	memset(sql_buf, 0, 2048);

	sql_op = op;
	for (tab_it = 0; tab_it < num_tables; tab_it++) {
		role = ROLE_PC;
		int connector = 1 << 0;
		connector |= 1 << 1;
		connector |= 1 << 2;
		conflict = CONFLICT_MAX;
		// P_TAB_REV_SUBMITTED($tab, $sid, PC) and P_TAB_NONCONFLICT_ROLE($tab,$sid,PC)
		g_sql_pred->create_table_submitted_review_pred(pred[0], 3, 
				TTYPE_VARLEN, tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1,
				TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role);
		g_sql_pred->create_table_nonconflict_role_pred(pred[1], 4, 
				TTYPE_VARLEN, tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1,
				TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role,
				TTYPE_INTEGER, &conflict);
		combine_pred(tmp_sql_buf[2], &pred_len, pred, 2, 0);

		for (i = 0; i < 3; i++)
			memset(pred[i], 0, 1024);

		// P_TAB_NONCONFLICT_ROLE($tab, $sid, CHAIR|ADMIN);
		role = ROLE_CHAIR|ROLE_ADMIN;
		g_sql_pred->create_table_nonconflict_role_pred(tmp_sql_buf[0], 4, 
				TTYPE_VARLEN, tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1,
				TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role,
				TTYPE_INTEGER, &conflict);
		// P_TAB_PAPER_MANAGER($tab, $sid)
		g_sql_pred->create_table_paper_manager(tmp_sql_buf[1], 2,
				TTYPE_VARLEN, tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1,
				TTYPE_VARIABLE, &sess_var);
		// P_TAB_REV_REQUESTED($tab, $sid)
		if ((strlen(tab_paper) != strlen(tab_list[tab_it])) ||
				strcmp(tab_paper, tab_list[tab_it]) != 0) {
			g_sql_pred->create_review_requester_pred(tmp_sql_buf[3], 1,
				TTYPE_VARIABLE, &sess_var);
			combine_pred(pred[1], &pred_len, tmp_sql_buf, 4, connector);
		} else {
			combine_pred(pred[1], &pred_len, tmp_sql_buf, 3, connector);
		}

		// P_TAB_PAPER_SUBMITTED($tab.pid)
		g_sql_pred->create_table_submitted_pred(pred[0], 2, TTYPE_VARLEN, tab_list[tab_it],
					tab_list[tab_it]+strlen(tab_list[tab_it])+1, TTYPE_INTEGER, &time_sub);
		
		if ((strlen(tab_paper) != strlen(tab_list[tab_it])) ||
				strcmp(tab_paper, tab_list[tab_it]) != 0) {
			g_sql_pred->create_is_review_submitted_pred(pred[2], 0);
			combine_pred(sql_buf, &pred_len, pred, 3, 0);
		} else {
			combine_pred(sql_buf, &pred_len, pred, 2, 0);
		}
		sql_buf_end = sql_buf + pred_len + 1;

		sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid[tab_it],
				TTYPE_VARLEN, sql_buf, sql_buf_end);
	}

	free(tid);
	for (i = 0; i < 3; i++)
		free(pred[i]);
	for (i = 0; i < 4; i++)
		free(tmp_sql_buf[i]);

	return (char *) sql_op;
}

/*
 * P1: 
 * Paper.{paperId, title, abstract}
 */
void 
qapla_create_paper_title_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 3;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "title_abstract");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 2 ==
	// session_is($k) and curr_time < T_SUB
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_lt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_sub);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 3 ==
	// session_is($k) and curr_time >= T_SUB
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_sub);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// ==
	uint64_t role;
	char *tab_paper = (char *) "Paper";
	uint64_t tid_paper = get_schema_table_id(schema, tab_paper);

	int pred_len = 0;
	char *pred[2];
	char sql_buf[1024], *sql_buf_end;
	int i;
	for (i = 0; i < 2; i++)
		pred[i] = (char *) malloc(1024);

	// == sql 1 ==
	// P_USER_PAPER_AUTHOR
	op_it = 0;
	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 1024);

	g_sql_pred->create_table_paper_author_pred(pred[0], 2, TTYPE_VARLEN, tab_paper,
			tab_paper+strlen(tab_paper)+1, TTYPE_VARIABLE, &sess_var);

	memset(sql_buf, 0, 1024);
	combine_pred(sql_buf, &pred_len, &pred[0], 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_paper, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// P_ROLE(CHAIR|PC|ADMIN)
	op_it++;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(pred[1], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	memset(sql_buf, 0, 1024);
	combine_pred(sql_buf, &pred_len, &pred[1], 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_paper, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 3 ==
	// P_ROLE(CHAIR|PC|ADMIN) and P_PAPER_SUBMITTED
	op_it++;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	memset(sql_buf, 0, 1024);
	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 1024);

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_paper_submitted_pred(pred[1], 1, TTYPE_INTEGER, &time_sub);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_paper, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// ==
	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);

	return;
}

/*
 * P2:
 * Paper.{authorInformation}
 */
void
qapla_create_paper_author_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "author");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);
	
	// == dlog 2 ==
	// session_is($k) and curr_time >= T_NOTIFY
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_notify);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// ==
	uint64_t role;
	char *tab_paper = (char *) "Paper";
	uint64_t tid_paper = get_schema_table_id(schema, tab_paper);
	char sql_buf[512], *sql_buf_end;
	char *pred[2];
	int pred_len;
	
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	// == sql 1 ==
	// P_USER_PAPER_AUTHOR or P_ROLE(CHAIR|ADMIN)
	op_it = 0;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;

	const char *tab_list[1] = {"Paper"};
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_user_paper_author(schema, g_sql_pred,
			sql_op, sess_var, (char **) tab_list, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// P_ROLE(CHAIR|PC|ADMIN) and P_PAPER_ACCEPTED
	op_it++;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_paper_accepted_pred(pred[1], 1, TTYPE_INTEGER, &time_sub);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_paper,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);

	return;
}

/*
 * P2:
 * Paper.{paperId, title, abstract, authorInformation}
 */
void
qapla_create_paper_title_author_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	qapla_create_paper_author_policy(schema, g_sql_pred, pol, pol_len);
	qapla_policy_t *qp = (qapla_policy_t *) pol;
	sprintf(qp->alias, "title_abstract_author");
}

/*
 * Topic(title)
 */
void
qapla_create_paper_title_topic_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 4;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "topic_title_link");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 2 ==
	// session_is($k) and curr_time < T_SUB
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_lt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_sub);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 3 ==
	// session_is($k) and curr_time >= T_SUB
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_sub);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 4 ==
	// session_is($k) and curr_time >= T_CONF
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &conf_time);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// ==
	uint64_t role;
	char *tab_paper = (char *) "Paper";
	uint64_t tid_paper = get_schema_table_id(schema, tab_paper);

	int pred_len = 0;
	char *pred[2];
	char sql_buf[1024], *sql_buf_end;
	int i;
	for (i = 0; i < 2; i++)
		pred[i] = (char *) malloc(1024);

	// == sql 1 ==
	// P_USER_PAPER_AUTHOR
	op_it = 0;
	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 1024);

	g_sql_pred->create_table_paper_author_pred(pred[0], 2, TTYPE_VARLEN, tab_paper,
			tab_paper+strlen(tab_paper)+1, TTYPE_VARIABLE, &sess_var);

	memset(sql_buf, 0, 1024);
	combine_pred(sql_buf, &pred_len, &pred[0], 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_paper, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// P_ROLE(CHAIR|PC|ADMIN)
	op_it++;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(pred[1], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	memset(sql_buf, 0, 1024);
	combine_pred(sql_buf, &pred_len, &pred[1], 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_paper, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 3 ==
	// P_ROLE(CHAIR|PC|ADMIN) and P_PAPER_SUBMITTED
	op_it++;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	memset(sql_buf, 0, 1024);
	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 1024);

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_paper_submitted_pred(pred[1], 1, TTYPE_INTEGER, &time_sub);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_paper, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 4 ==
	// true
	// XXX: should check for role of logged in user
	op_it++;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_true_op(schema, g_sql_pred,
			sql_op, tab_paper);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// ==
	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);

	return;
}

/*
 * Affiliation(authorInformation)
 */
void
qapla_create_paper_author_affil_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 3;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "affil_author_link");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);
	
	// == dlog 2 ==
	// session_is($k) and curr_time >= T_NOTIFY
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_notify);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 3 ==
	// session_is($k) and curr_time >= T_CONF
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &conf_time);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// ==
	uint64_t role;
	char *tab_paper = (char *) "Paper";
	uint64_t tid_paper = get_schema_table_id(schema, tab_paper);
	char sql_buf[1024], *sql_buf_end;
	char *pred[2];
	int pred_len;
	
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}
	memset(sql_buf, 0, 1024);

	// == sql 1 ==
	// P_USER_PAPER_AUTHOR
	op_it = 0;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;

	const char *tab_list[1] = {"Paper"};
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_user_paper_author(schema, g_sql_pred,
			sql_op, sess_var, (char **) tab_list, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// P_ROLE(CHAIR|PC|ADMIN) and P_PAPER_ACCEPTED
	op_it++;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_paper_accepted_pred(pred[1], 1, TTYPE_INTEGER, &time_sub);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_paper,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 3 ==
	// true
	op_it++;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_true_op(schema, g_sql_pred,
			sql_op, tab_paper);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);

	return;
}

void
_qapla_create_paper_analytics_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	
	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k) and curr_time >= T_CONF
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &conf_time);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	uint64_t role;
	char *tab_paper = (char *) "Paper";
	uint64_t tid_paper = get_schema_table_id(schema, tab_paper);
	char *pred[1];
	char sql_buf[512], *sql_buf_end;
	int pred_len = 0;
	int i;
	for (i = 0; i < 1; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	role = ROLE_CHAIR|ROLE_ANALYST|ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_paper,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 1; i++)
		free(pred[i]);

	return;
}

void
qapla_create_paper_aggr_topic_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	_qapla_create_paper_analytics_policy(schema, g_sql_pred, pol, pol_len);
	qapla_policy_t *qp = (qapla_policy_t *) pol;
	sprintf(qp->alias, "aggr_by_topic");
}

void
qapla_create_paper_aggr_affil_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	_qapla_create_paper_analytics_policy(schema, g_sql_pred, pol, pol_len);
	qapla_policy_t *qp = (qapla_policy_t *) pol;
	sprintf(qp->alias, "aggr_by_affil");
}

void
qapla_create_paper_aggr_topic_country_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	_qapla_create_paper_analytics_policy(schema, g_sql_pred, pol, pol_len);
	qapla_policy_t *qp = (qapla_policy_t *) pol;
	sprintf(qp->alias, "aggr_by_country");
}

/*
 * P3:
 * Paper.{outcome}
 */
void
qapla_create_paper_outcome_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "outcome");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// session_is($k) and curr_time >= T_NOTIFY
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_notify);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it++;
	// == dlog 2 ==
	// session_is($k) 
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// ==
	uint64_t role, conflict;
	char *tab_paper = (char *) "Paper";
	uint64_t tid_paper = get_schema_table_id(schema, tab_paper);

	int connector = 0;
	int pred_len = 0;
	char *pred[3];
	char *tmp_sql_buf[2];
	char sql_buf[2048], *sql_buf_end;
	int i;
	for (i = 0; i < 3; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}
	for (i = 0; i < 2; i++) {
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	memset(sql_buf, 0, 2048);

	// == sql 1 ==
	// P_USER_PAPER_AUTHOR or P_ROLE(CHAIR|PC|ADMIN)
	op_it = 0;

	g_sql_pred->create_table_paper_author_pred(pred[0], 2, TTYPE_VARLEN, tab_paper,
			tab_paper+strlen(tab_paper)+1, TTYPE_VARIABLE, &sess_var);
	combine_pred(tmp_sql_buf[0], &pred_len, pred, 1, 0);

	for (i = 0; i < 3; i++)
		memset(pred[i], 0, 1024);

	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_paper_submitted_pred(pred[1], 1, TTYPE_INTEGER, &time_sub);
	combine_pred(tmp_sql_buf[1], &pred_len, pred, 2, 0);
	
	connector |= 1;
	combine_pred(sql_buf, &pred_len, tmp_sql_buf, 2, connector);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_paper, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// P_TAB_PAPER_SUBMITTED(Paper) and 
	// (P_TAB_NONCONFLICT_ROLE(CHAIR|PC, Paper) or P_TAB_PAPER_MANAGER(Paper)
	//  or P_ROLE(ADMIN))
	op_it++;
	connector = 0;
	role = ROLE_CHAIR|ROLE_PC;
	conflict = CONFLICT_MAX;

	memset(sql_buf, 0, 2048);
	for (i = 0; i < 3; i++)
		memset(pred[i], 0, 1024);
	for (i = 0; i < 2; i++)
		memset(tmp_sql_buf[i], 0, 1024);

	g_sql_pred->create_paper_submitted_pred(pred[0], 1, TTYPE_INTEGER, &time_sub);
	combine_pred(tmp_sql_buf[0], &pred_len, pred, 1, 0);

	memset(pred[0], 0, 1024);
	g_sql_pred->create_table_nonconflict_role_pred(pred[0], 4, TTYPE_VARLEN,
			tab_paper, tab_paper+strlen(tab_paper)+1, TTYPE_VARIABLE, &sess_var,
			TTYPE_INTEGER, &role, TTYPE_INTEGER, &conflict);
	g_sql_pred->create_table_paper_manager(pred[1], 2, TTYPE_VARLEN,
			tab_paper, tab_paper+strlen(tab_paper)+1, TTYPE_VARIABLE, &sess_var);
	role = ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(pred[2], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	connector = 1;
	connector = (1 << 1);
	combine_pred(tmp_sql_buf[1], &pred_len, pred, 3, connector);

	combine_pred(sql_buf, &pred_len, tmp_sql_buf, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_paper, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 3; i++)
		free(pred[i]);
	for (i = 0; i < 2; i++)
		free(tmp_sql_buf[i]);

	return;
}

/*
 * Paper.{shepherdContactId}
 */
void
qapla_create_paper_shepherd_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "shepherd");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// session_is($k) and curr_time >= T_NOTIFY
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_notify);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it++;
	// == dlog 2 ==
	// session_is($k) 
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// ==
	uint64_t role, conflict;
	char *tab_paper = (char *) "Paper";
	uint64_t tid_paper = get_schema_table_id(schema, tab_paper);

	int connector = 0;
	int pred_len = 0;
	char *pred[3];
	char *tmp_sql_buf[2];
	char sql_buf[2048], *sql_buf_end;
	int i;
	for (i = 0; i < 3; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}
	for (i = 0; i < 2; i++) {
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	memset(sql_buf, 0, 2048);

	// == sql 1 ==
	// P_USER_PAPER_AUTHOR 
	op_it = 0;

	g_sql_pred->create_table_paper_author_pred(pred[0], 2, TTYPE_VARLEN, tab_paper,
			tab_paper+strlen(tab_paper)+1, TTYPE_VARIABLE, &sess_var);

	combine_pred(sql_buf, &pred_len, pred, 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_paper, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// P_TAB_PAPER_SUBMITTED(Paper) and 
	// (P_TAB_NONCONFLICT_ROLE(PC, Paper) or P_TAB_PAPER_MANAGER(Paper)
	//  or P_ROLE(CHAIR|ADMIN))
	op_it++;
	connector = 0;

	memset(sql_buf, 0, 2048);
	for (i = 0; i < 3; i++)
		memset(pred[i], 0, 1024);
	for (i = 0; i < 2; i++)
		memset(tmp_sql_buf[i], 0, 1024);

	g_sql_pred->create_paper_submitted_pred(pred[0], 1, TTYPE_INTEGER, &time_sub);
	combine_pred(tmp_sql_buf[0], &pred_len, pred, 1, 0);

	role = ROLE_PC;
	conflict = CONFLICT_MAX;

	memset(pred[0], 0, 1024);
	g_sql_pred->create_table_nonconflict_role_pred(pred[0], 4, TTYPE_VARLEN,
			tab_paper, tab_paper+strlen(tab_paper)+1, TTYPE_VARIABLE, &sess_var,
			TTYPE_INTEGER, &role, TTYPE_INTEGER, &conflict);
	g_sql_pred->create_table_paper_manager(pred[1], 2, TTYPE_VARLEN,
			tab_paper, tab_paper+strlen(tab_paper)+1, TTYPE_VARIABLE, &sess_var);

	role = ROLE_CHAIR|ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(pred[2], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	connector = 1 | (1 << 1);
	combine_pred(tmp_sql_buf[1], &pred_len, pred, 3, connector);

	combine_pred(sql_buf, &pred_len, tmp_sql_buf, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_paper, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 3; i++)
		free(pred[i]);
	for (i = 0; i < 2; i++)
		free(tmp_sql_buf[i]);

	return;
}

void
qapla_create_aggr_group_outcome_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "aggr_outcome");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// curr_time >= T_NOTIFY
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_notify);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// public
	char *tab_paper = (char *) "Paper";

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_true_op(schema, g_sql_pred,
			sql_op, tab_paper);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	return;
}

/*
 * P4:
 * Paper.{leadContactId}
 */
void
qapla_create_paper_lead_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "lead");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// session_is($k) and curr_time >= T_REV_START and curr_time < T_DISCUSS_START
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_start);
	dlog_op = create_lt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it++;
	// == dlog 2 ==
	// session_is($k) and curr_time >= T_DISCUSS_START
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// ==
	uint64_t role, conflict;
	char *tab_paper = (char *) "Paper";
	uint64_t tid_paper = get_schema_table_id(schema, tab_paper);

	char *pred[2];
	char * tmp_sql_buf[3];
	char sql_buf[2048], *sql_buf_end;
	int pred_len;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}
	for (i = 0; i < 3; i++) {
		tmp_sql_buf[i] = (char *) malloc(512);
		memset(tmp_sql_buf[i], 0, 512);
	}
	memset(sql_buf, 0, 2048);

	// == sql 1 ==
	// (P_TAB_PAPER_SUBMITTED(Paper) and 
	// 	(P_TAB_NONCONFLICT_ROLE(CHAIR|ADMIN, Paper) or P_TAB_PAPER_MANAGER(Paper) or
	// 		(P_TAB_SUBMITTED_REVIEW(Paper) and P_TAB_NONCONFLICT_ROLE(PC, Paper)))
	op_it = 0;
	
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_review_phase_op(schema, g_sql_pred,
			sql_op, sess_var, (char **) &tab_paper, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// (P_TAB_PAPER_SUBMITTED(Paper) and
	// 	(P_TAB_NONCONFLICT_ROLE(CHAIR|PC|ADMIN, Paper) or P_TAB_PAPER_MANAGER(Paper))
	op_it++;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	conflict = CONFLICT_MAX;

	g_sql_pred->create_paper_submitted_pred(pred[0], 1, TTYPE_INTEGER, &time_sub);

	g_sql_pred->create_table_nonconflict_role_pred(tmp_sql_buf[0], 4, 
			TTYPE_VARLEN, tab_paper, tab_paper+strlen(tab_paper)+1, TTYPE_VARIABLE,
			&sess_var, TTYPE_INTEGER, &role, TTYPE_INTEGER, &conflict);
	g_sql_pred->create_table_paper_manager(tmp_sql_buf[1], 2,
			TTYPE_VARLEN, tab_paper, tab_paper+strlen(tab_paper)+1,
			TTYPE_VARIABLE, &sess_var);

	combine_pred(pred[1], &pred_len, tmp_sql_buf, 2, 1);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;
	
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_paper,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);
	for (i = 0; i < 3; i++)
		free(tmp_sql_buf[i]);

	return;
}

void
qapla_create_paper_manager_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "manager");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// ==
	uint64_t role, conflict;
	char *tab_paper = (char *) "Paper";
	uint64_t tid_paper = get_schema_table_id(schema, tab_paper);

	char *pred[2];
	char * tmp_sql_buf[3];
	char sql_buf[2048], *sql_buf_end;
	int pred_len;
	int connector = 0;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}
	for (i = 0; i < 3; i++) {
		tmp_sql_buf[i] = (char *) malloc(512);
		memset(tmp_sql_buf[i], 0, 512);
	}
	memset(sql_buf, 0, 2048);

	// == sql ==
	// (P_TAB_PAPER_SUBMITTED(Paper) and
	// 	(ROLE(CHAIR|ADMIN) or P_TAB_NONCONFLICT_ROLE(PC, Paper) or 
	// 	 P_TAB_PAPER_MANAGER(Paper))

	g_sql_pred->create_paper_submitted_pred(pred[0], 1, TTYPE_INTEGER, &time_sub);

	role = ROLE_PC;
	conflict = CONFLICT_MAX;
	g_sql_pred->create_table_nonconflict_role_pred(tmp_sql_buf[0], 4, 
			TTYPE_VARLEN, tab_paper, tab_paper+strlen(tab_paper)+1, TTYPE_VARIABLE,
			&sess_var, TTYPE_INTEGER, &role, TTYPE_INTEGER, &conflict);
	g_sql_pred->create_table_paper_manager(tmp_sql_buf[1], 2,
			TTYPE_VARLEN, tab_paper, tab_paper+strlen(tab_paper)+1,
			TTYPE_VARIABLE, &sess_var);

	role = ROLE_CHAIR|ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(tmp_sql_buf[2], 2,
			TTYPE_INTEGER, &role, TTYPE_VARIABLE, &sess_var);

	connector = 1 | (1 << 1);
	combine_pred(pred[1], &pred_len, tmp_sql_buf, 3, connector);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;
	
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_paper,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++) {
		free(pred[i]);
	}
	for (i = 0; i < 3; i++) {
		free(tmp_sql_buf[i]);
	}

	return;
}

void
qapla_create_paper_meta_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "paper_meta");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it++;
	// == dlog 2 ==
	// session_is($k) and curr_time >= T_SUB
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_sub);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	uint64_t role;
	char *tab_contact = (char *) "ContactInfo";
	const char *tab_list[1] = {"Paper"};
	uint64_t tid_contact = get_schema_table_id(schema, tab_contact);
	uint64_t tid_paper = get_schema_table_id(schema, (char *) tab_list[0]);
	char sql_buf[512], *sql_buf_end;
	char *pred[2];
	int pred_len = 0;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	// == sql 1 ==
	// P_TAB_USER_PAPER_AUTHOR(Paper)
	op_it = 0;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;

	sql_op = (dlog_pi_op_t *) _qapla_create_sql_user_paper_author(schema, g_sql_pred,
			sql_op, sess_var, (char **) tab_list, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// P_ROLE(CHAIR|PC|ADMIN) and P_TAB_PAPER_SUBMITTED(Paper)
	op_it++;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_table_submitted_pred(pred[1], 2, TTYPE_VARLEN, tab_list[0],
			tab_list[0]+strlen(tab_list[0])+1, TTYPE_INTEGER, &time_sub);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_paper,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);

	return;
}

/*
 * PS1: 
 * PaperStorage.{paperId, paper, paperStorageId, originalStorageId}
 */
void 
qapla_create_pstore_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "pstore");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it++;
	// == dlog 2 ==
	// session_is($k) and curr_time >= T_SUB
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_sub);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	uint64_t role;
	const char *tab_list[1] = {"PaperStorage"};
	uint64_t tid_pstore = get_schema_table_id(schema, (char *) tab_list[0]);
	char sql_buf[512], *sql_buf_end;
	char *pred[2];
	int pred_len = 0;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	// == sql 1 ==
	// P_TAB_USER_PAPER_AUTHOR(PaperStorage)
	op_it = 0;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;

	sql_op = (dlog_pi_op_t *) _qapla_create_sql_user_paper_author(schema, g_sql_pred,
			sql_op, sess_var, (char **) tab_list, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// P_ROLE(CHAIR|PC|ADMIN) and P_TAB_PAPER_SUBMITTED(PaperStorage)
	op_it++;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_table_submitted_pred(pred[1], 2, TTYPE_VARLEN, tab_list[0],
			tab_list[0]+strlen(tab_list[0])+1, TTYPE_INTEGER, &time_sub);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_pstore,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);

	return;
}

char *
_qapla_create_sql_discuss_phase_op(db_t *schema, sql_pred_t *g_sql_pred,
		dlog_pi_op_t *op, uint16_t sess_var, char **tab_list, int num_tables)
{
	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;

	uint64_t *tid = (uint64_t *) malloc(sizeof(uint64_t) * num_tables);
	memset(tid, 0, sizeof(uint64_t) * num_tables);
	int tab_it;
	for (tab_it = 0; tab_it < num_tables; tab_it++) {
		tid[tab_it] = get_schema_table_id(schema, tab_list[tab_it]);
	}

	uint64_t role, conflict;
	char *pred[3];
	char * tmp_sql_buf[3];
	char sql_buf[2048], *sql_buf_end;
	int pred_len;
	int i;
	for (i = 0; i < 3; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}
	for (i = 0; i < 3; i++) {
		tmp_sql_buf[i] = (char *) malloc(512);
		memset(tmp_sql_buf[i], 0, 512);
	}
	memset(sql_buf, 0, 2048);

	sql_op = op;
	for (tab_it = 0; tab_it < num_tables; tab_it++) {
		// P_TAB_PAPER_SUBMITTED($tab)
		g_sql_pred->create_table_submitted_pred(pred[0], 2, TTYPE_VARLEN,
				tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1,
				TTYPE_INTEGER, &time_sub);
		//g_sql_pred->create_is_review_submitted_pred(pred[1], 0);

		// P_TAB_NONCONFLICT_ROLE($tab, $sid, CHAIR|PC|ADMIN)
		// admin may not have conflicts at all, condition for admin can be simplified
		// but then chair may also be the admin..
		role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
		conflict = CONFLICT_MAX;
		g_sql_pred->create_table_nonconflict_role_pred(tmp_sql_buf[0], 4, 
				TTYPE_VARLEN, tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1,
				TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role,
				TTYPE_INTEGER, &conflict);
		// P_TAB_PAPER_MANAGER($tab.pid)
		g_sql_pred->create_table_paper_manager(tmp_sql_buf[1], 2,
				TTYPE_VARLEN, tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1,
				TTYPE_VARIABLE, &sess_var);
		// P_TAB_REV_REQUESTED($tab, $sid)
		g_sql_pred->create_review_requester_pred(tmp_sql_buf[2], 1,
				TTYPE_VARIABLE, &sess_var);
		combine_pred(pred[1], &pred_len, tmp_sql_buf, 3, 1);
		
		combine_pred(sql_buf, &pred_len, pred, 2, 0);
		sql_buf_end = sql_buf + pred_len + 1;

		sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid[tab_it],
				TTYPE_VARLEN, sql_buf, sql_buf_end);
	}

	free(tid);
	for (i = 0; i < 3; i++)
		free(pred[i]);
	for (i = 0; i < 3; i++)
		free(tmp_sql_buf[i]);

	return (char *) sql_op;
}

/* 
 * R1:
 * PaperReview.{overAllMerit, reviewerQualification, novelty, technicalMerit, 
 * strengthOfPaper, weaknessOfPaper, paperSummary, commentsToAuthor, 
 * commentsToAddress}
 */
void
qapla_create_review_for_author_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 3;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "review_public");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	
	// == dlog 1 ==
	// session_is($k) and curr_time >= T_REV_START and curr_time < T_DISCUSS
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_start);
	dlog_op = create_lt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it++;
	// == dlog 2 == 
	// session_is($k) and curr_time >= T_DISCUSS
	// don't add LT pred here, since the PC policy is not included in the next clause
	// and must be added during policy eval
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it++;
	// == dlog 3 ==
	// session_is($k) and curr_time >= T_NOTIFY
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_notify);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	char *tab_review = (char *) "PaperReview";
	uint64_t tid_review = (uint64_t) get_schema_table_id(schema, tab_review);
	char *pred[3];
	char sql_buf[1024], *sql_buf_end;
	int pred_len = 0;
	memset(sql_buf, 0, 1024);
	int i;
	for (i = 0; i < 3; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}

	// == sql 1 ==
	// (P_TAB_PAPER_SUBMITTED(PaperReview) and reviewSubmitted > 0 and 
	//  reviewNeedsSubmit <= 0 and
	//  (P_TAB_NONCONFLICT_ROLE(CHAIR|ADMIN, PaperReview) or 
	//  	requestedBy=$k or
	// 		P_TAB_PAPER_MANAGER(PaperReview) or
	// 		(P_TAB_SUBMITTED_REVIEW(PaperReview) and 
	// 			P_TAB_NONCONFLICT_ROLE(PC, PaperReview))))
	op_it = 0;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_review_phase_op(schema, g_sql_pred,
			sql_op, sess_var, (char **) &tab_review, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// (P_TAB_PAPER_SUBMITTED(PaperReview) and //(commented) reviewSubmitted > 0 and
	// 	//(commented) reviewNeedsSubmit <= 0 and
	// 	(P_TAB_NONCONFLICT_ROLE(CHAIR|PC|ADMIN, PaperReview) or
	// 		P_TAB_PAPER_MANAGER(PaperReview) or requestedBy = $k))
	op_it++;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_discuss_phase_op(schema, g_sql_pred,
			sql_op, sess_var, (char **) &tab_review, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 3 ==
	// reviewSubmitted > 0 and
	// reviewNeedsSubmit <= 0 and P_TAB_USER_PAPER_AUTHOR(PaperReview)
	op_it++;
	//g_sql_pred->create_table_submitted_pred(pred[0], 2, TTYPE_VARLEN, tab_review,
	//		tab_review+strlen(tab_review)+1, TTYPE_INTEGER, &time_sub);
	g_sql_pred->create_is_review_submitted_pred(pred[0], 0);
	g_sql_pred->create_table_paper_author_pred(pred[1], 2, TTYPE_VARLEN, tab_review,
			tab_review+strlen(tab_review)+1, TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_review, 
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 3; i++)
		free(pred[i]);

	return;
}

/*
 * R2:
 * PaperReview.{reviewId, contactId, reviewToken, reviewSubmitted, 
 * reviewModified, reviewNeedsSubmit, reviewOrdinal, reviewEditVersion,
 * commentsToPC}
 */
void
qapla_create_review_private_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "review_private");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	
	// == dlog 1 == 
	// session_is($k) and curr_time >= T_REV_START and curr_time < T_DISCUSS
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_start);
	dlog_op = create_lt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 2 == 
	// session_is($k) and curr_time >= T_DISCUSS
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// ==
	char *tab_review = (char *) "PaperReview";

	// == sql 1 ==
	// (P_TAB_PAPER_SUBMITTED(PaperReview) and
	//  (P_TAB_NONCONFLICT_ROLE(CHAIR|ADMIN, PaperReview) or 
	// 		P_TAB_PAPER_MANAGER(PaperReview) or
	// 		(P_TAB_SUBMITTED_REVIEW(PaperReview) and 
	// 			P_TAB_NONCONFLICT_ROLE(PC, PaperReview))))
	op_it = 0;
	
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_review_phase_op(schema, g_sql_pred,
			sql_op, sess_var, (char **) &tab_review, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	// == sql 2 ==
	// (P_TAB_PAPER_SUBMITTED(PaperReview) and
	// 	(P_TAB_NONCONFLICT_ROLE(CHAIR|PC|ADMIN, PaperReview) or
	// 		P_TAB_PAPER_MANAGER(PaperReview)))
	op_it++;
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_discuss_phase_op(schema, g_sql_pred,
			sql_op, sess_var, (char **) &tab_review, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	return;
}

void
qapla_create_aggr_review_status_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "aggr_rev_status");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	
	// == dlog == 
	// session_is($k) and curr_time >= T_REV_START
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_start);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// P_TAB_PAPER_SUBMITTED(PaperReview) and
	// (P_TAB_NONCONFLICT_ROLE(CHAIR|PC|ADMIN, PaperReview) or 
	// 	P_TAB_PAPER_MANAGER(PaperReview))
	char *tab_review = (char *) "PaperReview";
	uint64_t tid_review = get_schema_table_id(schema, tab_review);
	uint64_t role, conflict;
	char *pred[2];
	char * tmp_sql_buf[2];
	char sql_buf[2048], *sql_buf_end;
	int pred_len;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(1024);
		tmp_sql_buf[i] = (char *) malloc(512);
		memset(pred[i], 0, 1024);
		memset(tmp_sql_buf[i], 0, 512);
	}
	memset(sql_buf, 0, 2048);

	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	conflict = CONFLICT_MAX;
	g_sql_pred->create_table_nonconflict_role_pred(tmp_sql_buf[0], 4,
			TTYPE_VARLEN, tab_review, tab_review+strlen(tab_review)+1,
			TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role, TTYPE_INTEGER, &conflict);
	g_sql_pred->create_table_paper_manager(tmp_sql_buf[1], 2,
			TTYPE_VARLEN, tab_review, tab_review+strlen(tab_review)+1,
			TTYPE_VARIABLE, &sess_var);
	combine_pred(pred[1], &pred_len, tmp_sql_buf, 2, 1);

	g_sql_pred->create_table_submitted_pred(pred[0], 2, TTYPE_VARLEN, tab_review,
			tab_review+strlen(tab_review)+1, TTYPE_INTEGER, &time_sub);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_review,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++) {
		free(pred[i]);
		free(tmp_sql_buf[i]);
	}

	return;
}

void
qapla_create_avg_overall_merit_pc_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "avg_merit_groupby_cid");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	
	// == dlog == 
	// session_is($k) and curr_time >= T_REV_START
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_start);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// P_TAB_PAPER_SUBMITTED(PaperReview) and 
	// (P_ROLE(CHAIR|PC|ADMIN) or P_TAB_PAPER_MANAGER(PaperReview))
	char *tab_review = (char *) "PaperReview";
	uint64_t tid_review = get_schema_table_id(schema, tab_review);
	uint64_t role;
	char *pred[2];
	char * tmp_sql_buf[2];
	char sql_buf[1024], *sql_buf_end;
	int pred_len;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(1024);
		tmp_sql_buf[i] = (char *) malloc(512);
		memset(pred[i], 0, 1024);
		memset(tmp_sql_buf[i], 0, 512);
	}
	memset(sql_buf, 0, 1024);

	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(tmp_sql_buf[0], 2,
			TTYPE_INTEGER, &role, TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_table_paper_manager(tmp_sql_buf[1], 2,
			TTYPE_VARLEN, tab_review, tab_review+strlen(tab_review)+1,
			TTYPE_VARIABLE, &sess_var);
	combine_pred(pred[1], &pred_len, tmp_sql_buf, 2, 1);

	g_sql_pred->create_table_submitted_pred(pred[0], 2, TTYPE_VARLEN, tab_review,
			tab_review+strlen(tab_review)+1, TTYPE_INTEGER, &time_sub);
	combine_pred(sql_buf, &pred_len, pred, 2, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_review,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++) {
		free(pred[i]);
		free(tmp_sql_buf[i]);
	}

	return;
}

void
_qapla_create_rid_or_cid_view_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *tname, char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	
	// == dlog 1 == 
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// ==
	uint64_t role;
	uint64_t tid = get_schema_table_id(schema, tname);
	char *pred[2];
	char sql_buf[1024], *sql_buf_end;
	int pred_len;

	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	// == sql 1 ==
	op_it = 0;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_table_paper_manager(pred[1], 2, TTYPE_VARLEN, tname,
			tname+strlen(tname)+1, TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 2, 1);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);

	return;
}

void
_qapla_create_review_pid_view_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *tname, char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	
	// == dlog 1 ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	uint64_t role;
	uint64_t tid = get_schema_table_id(schema, tname);
	char *pred[2];
	char *tmp_sql_buf[2];
	char sql_buf[1024], *sql_buf_end;
	int pred_len;

	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	memset(sql_buf, 0, 1024);

	// == sql 1 ==
	// (P_TAB_USER_PAPER_AUTHOR(PaperReview) or
	// 	(P_TAB_PAPER_SUBMITTED(PaperReview) and 
	// 		(P_ROLE(CHAIR|PC|ADMIN) or P_TAB_PAPER_MANAGER(PaperReview))))
	op_it = 0;
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_table_paper_manager(pred[1], 2, TTYPE_VARLEN, tname,
			tname+strlen(tname)+1, TTYPE_VARIABLE, &sess_var);
	combine_pred(tmp_sql_buf[1], &pred_len, pred, 2, 1);

	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 1024);

	g_sql_pred->create_table_submitted_pred(tmp_sql_buf[0], 2, TTYPE_VARLEN,
			tname, tname+strlen(tname)+1, TTYPE_INTEGER, &time_sub);
	combine_pred(pred[1], &pred_len, tmp_sql_buf, 2, 0);

	g_sql_pred->create_table_paper_author_pred(pred[0], 2,
			TTYPE_VARLEN, tname, tname+strlen(tname)+1,
			TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 2, 1);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid, 
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++) {
		free(pred[i]);
		free(tmp_sql_buf[i]);
	}
}

void
_qapla_create_review_paper_analytics_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	
	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k) and curr_time >= T_CONF
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &conf_time);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	uint64_t role;
	char *tab_paper = (char *) "Paper";
	char *tab_review = (char *) "PaperReview";
	uint64_t tid_paper = get_schema_table_id(schema, tab_paper);
	uint64_t tid_review = get_schema_table_id(schema, tab_review);
	char *pred[1];
	char sql_buf[512], *sql_buf_end;
	int pred_len = 0;
	int i;
	for (i = 0; i < 1; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	role = ROLE_CHAIR|ROLE_ANALYST|ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_paper,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_review,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 1; i++)
		free(pred[i]);

	return;
}

void
qapla_create_review_paper_aggr_topic_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	_qapla_create_review_paper_analytics_policy(schema, g_sql_pred, pol, pol_len);
	qapla_policy_t *qp = (qapla_policy_t *) pol;
	sprintf(qp->alias, "review_aggr_by_topic");
}

void
qapla_create_review_paper_aggr_affil_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	_qapla_create_review_paper_analytics_policy(schema, g_sql_pred, pol, pol_len);
	qapla_policy_t *qp = (qapla_policy_t *) pol;
	sprintf(qp->alias, "review_aggr_by_affil");
}

/*
 * RR1:
 * ReviewRating.{reviewId, contactId, rating}
 */
void
qapla_create_review_rating_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "review_rating");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// session_is($k) and curr_time >= T_REV_START and curr_time < T_DISCUSS
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_start);
	dlog_op = create_lt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it++;
	// == dlog 2 ==
	// session_is($k) and curr_time >= T_DISCUSS_START
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// ==
	uint64_t role;
	char *tab_rrate = (char *) "ReviewRating";
	uint64_t tid_rrate = get_schema_table_id(schema, tab_rrate);
	
	char sql_buf[1024], *sql_buf_end;
	char *pred[2];
	char *tmp_sql_buf[3];
	int pred_len;
	int i;

	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	for (i = 0; i < 3; i++) {
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	memset(sql_buf, 0, 1024);

	op_it = 0;
	// == sql 1 ==
	role = ROLE_PC;
	g_sql_pred->create_rrating_submitted_review_pred(pred[0], 2, TTYPE_VARIABLE,
			&sess_var, TTYPE_INTEGER, &role);
	g_sql_pred->create_rrating_nonconflict_role_pred(pred[1], 2, TTYPE_VARIABLE,
			&sess_var, TTYPE_INTEGER, &role);
	combine_pred(tmp_sql_buf[2], &pred_len, pred, 2, 0);

	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 512);

	role = ROLE_CHAIR|ROLE_ADMIN;
	g_sql_pred->create_rrating_nonconflict_role_pred(tmp_sql_buf[0], 2,
			TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role);
	g_sql_pred->create_rrating_paper_manager(tmp_sql_buf[1], 2,
			TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role);
	
	int connector = 1;
	connector = (1 << 1);
	combine_pred(sql_buf, &pred_len, tmp_sql_buf, 3, connector);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_rrate,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 512);
	for (i = 0; i < 3; i++)
		memset(tmp_sql_buf[i], 0, 1024);
	memset(sql_buf, 0, 1024);

	op_it++;
	// == sql 2 ==
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	g_sql_pred->create_rrating_nonconflict_role_pred(tmp_sql_buf[0], 2,
			TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role);
	g_sql_pred->create_rrating_paper_manager(tmp_sql_buf[1], 2,
			TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role);
	combine_pred(sql_buf, &pred_len, tmp_sql_buf, 2, 1);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_rrate,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);
	for (i = 0; i < 3; i++)
		free(tmp_sql_buf[i]);

	return;
}

/*
 * PC1:
 * PaperConflict.{paperId, contactId, conflictType}
 */
void
qapla_create_conflict_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "conflict");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint64_t auth_conflict = CONFLICT_AUTHOR;
	uint64_t chair_mark = CONFLICT_CHAIRMARK;
	
	// == dlog 1 == 
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == dlog 2 ==
	// curr_time >= T_NOTIFY and session_is($k)
	op_it++;
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_gt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_notify);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);
	
	char *tab_conflict = (char *) "PaperConflict";
	uint64_t tid_conflict = (uint64_t) get_schema_table_id(schema, tab_conflict);
	uint64_t role = 0;
	char sql_buf[1024], *sql_buf_end;
	int pred_len = 0;
	int connector = 0;
	char *pred[2];
	char *tmp_sql_buf[3];
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	for (i = 0; i < 3; i++) {
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	memset(sql_buf, 0, 1024);

	// == sql 1 ==
	// (P_ROLE(CHAIR|ADMIN)) or
	// (PaperConflict.contactId=ContactInfo.contactId and email=$k and role=PC and
	//  /* conflictType < chairmark) or */
	// (exists 1 in Paper where pid=PaperConflict.pid and authorInformation like $k)
	
	op_it = 0;
	role = ROLE_CHAIR|ROLE_ADMIN;
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	combine_pred(tmp_sql_buf[0], &pred_len, pred, 1, 0);

	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 512);

	role = ROLE_PC;
	g_sql_pred->create_pc_role_conflict_pred(pred[0], 2, TTYPE_VARIABLE, &sess_var,
			TTYPE_INTEGER, &role);
	combine_pred(tmp_sql_buf[1], &pred_len, pred, 1, 0);

	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 512);

	g_sql_pred->create_author_conflict_pred(tmp_sql_buf[2], 1,
			TTYPE_VARIABLE, &sess_var);

	connector |= 1;
	connector |= (1 << 1);

	combine_pred(sql_buf, &pred_len, tmp_sql_buf, 3, connector);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_conflict,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	for (i = 0; i < 2; i++)
		memset(pred[i], 0, 512);
	// == sql 2 ==
	// P_ROLE(CHAIR|ADMIN|PC)
	op_it++;
	role = ROLE_CHAIR|ROLE_ADMIN|ROLE_PC;
	
	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_conflict,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++) {
		free(pred[i]);
	}
	for (i = 0; i < 3; i++) {
		free(tmp_sql_buf[i]);
	}

	return;
}

void
_qapla_create_table_chair_pc_manager_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *tname, char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// P_ROLE(CHAIR|PC|ADMIN) or P_TABLE_PAPER_MANAGER(tname)
	uint64_t role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	uint64_t tid = get_schema_table_id(schema, tname);

	char sql_buf[512], *sql_buf_end;
	char *pred[2];
	int pred_len = 0;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_table_paper_manager(pred[1], 2, TTYPE_VARLEN, tname,
			tname+strlen(tname)+1, TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 2, 1);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);

	return;
}

/*
 * PT1:
 * PaperTag.*
 */
void
qapla_create_papertags_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	_qapla_create_table_chair_pc_manager_policy(schema, g_sql_pred,
			(char *) "PaperTag", pol, pol_len);
	qapla_policy_t *qp = (qapla_policy_t *) pol;
	sprintf(qp->alias, "tags");
}

void
qapla_create_paper_options_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "options");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// P_ROLE(CHAIR|PC|ADMIN) or P_TABLE_PAPER_MANAGER(tname) or
	// P_TAB_USER_PAPER_AUTHOR(PaperOptions)
	uint64_t role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	char *tab_options = (char *) "PaperOption";
	uint64_t tid_options = get_schema_table_id(schema, tab_options);
	int connector = 0;

	char sql_buf[1024], *sql_buf_end;
	char *pred[3];
	int pred_len = 0;
	int i;
	for (i = 0; i < 3; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}
	memset(sql_buf, 0, 1024);

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_table_paper_manager(pred[1], 2, TTYPE_VARLEN, tab_options,
			tab_options+strlen(tab_options)+1, TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_table_paper_author_pred(pred[2], 2, TTYPE_VARLEN,
			tab_options, tab_options+strlen(tab_options)+1, TTYPE_VARIABLE, &sess_var);
	connector = 1;
	connector |= (1<<1);
	combine_pred(sql_buf, &pred_len, pred, 3, connector);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_options, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 3; i++)
		free(pred[i]);

	return;
}

char *
_qapla_create_sql_comment_review_phase_op(db_t *schema, sql_pred_t *g_sql_pred,
		dlog_pi_op_t *op, uint16_t sess_var, char **tab_list, int num_tables)
{
	char *read_sql_op;
	dlog_pi_op_t *sql_op;

	uint64_t *tid = (uint64_t *) malloc(sizeof(uint64_t) * num_tables);
	memset(tid, 0, sizeof(uint64_t) * num_tables);
	int tab_it;
	for (tab_it = 0; tab_it < num_tables; tab_it++) {
		tid[tab_it] = get_schema_table_id(schema, tab_list[tab_it]);
	}

	uint64_t role, ctype, conflict;
	uint64_t op_type;

	char sql_buf[2048], *sql_buf_end;
	char *pred[3];
	char *tmp_sql_buf[3];
	int pred_len = 0;
	int connector = 0;
	int i;
	for (i = 0; i < 3; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	memset(sql_buf, 0, 2048);

	sql_op = op;
	// == sql ==
	// P_TAB_PAPER_SUBMITTED(PaperComment) and commentType != DRAFT and
	// (P_TAB_PAPER_MANAGER(PaperComment) or 
	//  P_TAB_NONCONFLICT_ROLE(CHAIR|ADMIN, PaperComment) or 
	//  (P_TAB_SUBMITTED_REVIEW(PaperComment) and 
	//   P_TAB_NONCONFLICT_ROLE(PC, PaperComment) and commentType >= PCONLY))
	for (tab_it = 0; tab_it < num_tables; tab_it++) {
		role = ROLE_PC;
		conflict = CONFLICT_MAX;
		g_sql_pred->create_table_submitted_review_pred(pred[0], 3, TTYPE_VARLEN,
				tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1, 
				TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role);
		g_sql_pred->create_table_nonconflict_role_pred(pred[1], 4, TTYPE_VARLEN,
				tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1, 
				TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role,
				TTYPE_INTEGER, &conflict);

		op_type = OP_GE;
		ctype = COMMENTTYPE_PCONLY;
		g_sql_pred->create_comment_compare_pred(pred[2], 2, TTYPE_INTEGER, &op_type, 
				TTYPE_INTEGER, &ctype);
		combine_pred(tmp_sql_buf[2], &pred_len, pred, 3, 0);

		for (i = 0; i < 3; i++)
			memset(pred[i], 0, 1024);

		g_sql_pred->create_table_paper_manager(tmp_sql_buf[0], 2, 
				TTYPE_VARLEN, tab_list[tab_it], 
				tab_list[tab_it]+strlen(tab_list[tab_it])+1, TTYPE_VARIABLE, &sess_var);
		role = ROLE_CHAIR|ROLE_ADMIN;
		g_sql_pred->create_table_nonconflict_role_pred(tmp_sql_buf[1], 4,
				TTYPE_VARLEN, tab_list[tab_it], 
				tab_list[tab_it]+strlen(tab_list[tab_it])+1,
				TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role,
				TTYPE_INTEGER, &conflict);
		
		connector = 1;
		connector |= (1 << 1);
		combine_pred(pred[2], &pred_len, tmp_sql_buf, 3, connector);

		for (i = 0; i < 3; i++)
			memset(tmp_sql_buf[i], 0, 1024);

		g_sql_pred->create_table_submitted_pred(pred[0], 2, TTYPE_VARLEN, 
				tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1, 
				TTYPE_INTEGER, &time_sub);

		op_type = OP_NE;
		ctype = COMMENTTYPE_DRAFT;
		g_sql_pred->create_comment_compare_pred(pred[1], 2, TTYPE_INTEGER, &op_type,
				TTYPE_INTEGER, &ctype);

		combine_pred(sql_buf, &pred_len, pred, 3, 0);
		sql_buf_end = sql_buf + pred_len + 1;

		sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid[tab_it],
				TTYPE_VARLEN, sql_buf, sql_buf_end);
	}

	free(tid);
	for (i = 0; i < 3; i++) {
		free(pred[i]);
		free(tmp_sql_buf[i]);
	}

	return (char *) sql_op;
}

char *
_qapla_create_sql_comment_discuss_phase_op(db_t *schema, sql_pred_t *g_sql_pred,
		dlog_pi_op_t *op, uint16_t sess_var, char **tab_list, int num_tables)
{
	char *read_sql_op;
	dlog_pi_op_t *sql_op;

	uint64_t *tid = (uint64_t *) malloc(sizeof(uint64_t) * num_tables);
	memset(tid, 0, sizeof(uint64_t) * num_tables);
	int tab_it;
	for (tab_it = 0; tab_it < num_tables; tab_it++) {
		tid[tab_it] = get_schema_table_id(schema, tab_list[tab_it]);
	}

	uint64_t role, ctype, op_type, conflict;

	char sql_buf[2048], *sql_buf_end;
	char *pred[3];
	char *tmp_sql_buf[3];
	int pred_len = 0;
	int connector = 0;
	int i;
	for (i = 0; i < 3; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	memset(sql_buf, 0, 2048);

	sql_op = op;
	// == sql ==
	// P_TAB_PAPER_SUBMITTED(PaperComment) and commentType != DRAFT and
	// (P_TAB_PAPER_MANAGER(PaperComment) or
	//  P_TAB_NONCONFLICT_ROLE(PC|CHAIR|ADMIN, PaperComment))
	for (tab_it = 0; tab_it < num_tables; tab_it++) {
		g_sql_pred->create_table_paper_manager(tmp_sql_buf[0], 2,
				TTYPE_VARLEN, tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1, 
				TTYPE_VARIABLE, &sess_var);

		role = ROLE_PC|ROLE_CHAIR|ROLE_ADMIN;
		conflict = CONFLICT_MAX;
		g_sql_pred->create_table_nonconflict_role_pred(tmp_sql_buf[1], 4,
				TTYPE_VARLEN, tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1,
				TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role,
				TTYPE_INTEGER, &conflict);

		connector = 1;
		combine_pred(pred[2], &pred_len, tmp_sql_buf, 2, connector);

		g_sql_pred->create_table_submitted_pred(pred[0], 2, TTYPE_VARLEN, 
				tab_list[tab_it], tab_list[tab_it]+strlen(tab_list[tab_it])+1, 
				TTYPE_INTEGER, &time_sub);

		op_type = OP_NE;
		ctype = COMMENTTYPE_DRAFT;
		g_sql_pred->create_comment_compare_pred(pred[1], 2, TTYPE_INTEGER, &op_type,
				TTYPE_INTEGER, &ctype);

		combine_pred(sql_buf, &pred_len, pred, 3, 0);
		sql_buf_end = sql_buf + pred_len + 1;

		sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid[tab_it],
				TTYPE_VARLEN, sql_buf, sql_buf_end);
	}

	free(tid);
	for (i = 0; i < 3; i++) {
		free(pred[i]);
		free(tmp_sql_buf[i]);
	}

	return (char *) sql_op;
}

/*
 * PaperComment.{commentId, paperId, timeModified, timeNotified, comment, 
 * paperStorageId, commentType, commentTags, commentRound, commentFormat, 
 * authorOrdinal}
 * Comm1
 */
void
qapla_create_comment_for_author_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 3;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "comment_public");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// curr_time >= T_REV_START and session_is($k) and curr_time < T_DISCUSS
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_start);
	dlog_op = create_lt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it++;
	// == dlog 2 ==
	// curr_time >= T_DISCUSS_START and session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it++;
	// == dlog 3 ==
	// curr_time >= T_NOTIFY and session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_notify);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	uint64_t role, ctype, op_type;
	char *tab_comment = (char *) "PaperComment";
	uint64_t tid_comment = get_schema_table_id(schema, tab_comment);

	char sql_buf[2048], *sql_buf_end;
	char *pred[3];
	char *tmp_sql_buf[3];
	int pred_len = 0;
	int connector = 0;
	int i;
	for (i = 0; i < 3; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	memset(sql_buf, 0, 2048);

	op_it = 0;
	// == sql 1 ==
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_comment_review_phase_op(schema,
			g_sql_pred, sql_op, sess_var, (char **) &tab_comment, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	op_it++;
	// == sql 2 ==
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_comment_discuss_phase_op(schema,
			g_sql_pred, sql_op, sess_var, (char **) &tab_comment, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	op_it++;
	// == sql 3 ==
	// P_TAB_PAPER_SUBMITTED(PaperComment) and commentType != DRAFT and
	// (P_TAB_USER_PAPER_AUTHOR(PaperComment) and commentType >= AUTHOR)
	g_sql_pred->create_table_paper_author_pred(tmp_sql_buf[0], 2,
			TTYPE_VARLEN, tab_comment, tab_comment+strlen(tab_comment)+1,
			TTYPE_VARIABLE, &sess_var);

	op_type = OP_GE;
	ctype = COMMENTTYPE_AUTHOR;
	g_sql_pred->create_comment_compare_pred(tmp_sql_buf[1], 2, 
			TTYPE_INTEGER, &op_type, TTYPE_INTEGER, &ctype);
	g_sql_pred->create_comment_ordinal_pred(tmp_sql_buf[2], 0);
	combine_pred(pred[2], &pred_len, tmp_sql_buf, 3, 0);

	g_sql_pred->create_table_submitted_pred(pred[0], 2, TTYPE_VARLEN, tab_comment,
			tab_comment+strlen(tab_comment)+1, TTYPE_INTEGER, &time_sub);

	op_type = OP_NE;
	ctype = COMMENTTYPE_DRAFT;
	g_sql_pred->create_comment_compare_pred(pred[1], 2, TTYPE_INTEGER, &op_type,
			TTYPE_INTEGER, &ctype);
	combine_pred(sql_buf, &pred_len, pred, 3, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_comment,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);
	
	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 3; i++) {
		free(pred[i]);
		free(tmp_sql_buf[i]);
	}

	return;
}

/*
 * PaperComment.{contactId, replyTo, ordinal}
 * Comm2
 */
void
qapla_create_private_comment_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 2;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "comment_private");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);
	uint16_t time_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog 1 ==
	// curr_time >= T_REV_START and session_is($k) and curr_time < T_DISCUSS
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_start);
	dlog_op = create_lt(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it++;
	// == dlog 2 ==
	// curr_time >= T_DISCUSS_START and session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_time_is(dlog_op, TTYPE_VARIABLE, &time_var);
	dlog_op = create_ge(dlog_op, TTYPE_VARIABLE, &time_var, TTYPE_INTEGER, &time_rev_discuss);
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	op_it = 0;

	char *tab_comment = (char *) "PaperComment";

	// == sql 1 ==
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_comment_review_phase_op(schema,
			g_sql_pred, sql_op, sess_var, (char **) &tab_comment, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	op_it++;
	// == sql 2 ==
	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = (dlog_pi_op_t *) _qapla_create_sql_comment_discuss_phase_op(schema,
			g_sql_pred, sql_op, sess_var, (char **) &tab_comment, 1);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	return;
}

/*
 * PaperComment.{reviewId}
 * PaperComment.{contactId, replyTo}
 * Comm3a, Comm3b
 */
void
qapla_create_comment_id_or_contact_id_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	_qapla_create_rid_or_cid_view_policy(schema, g_sql_pred,
			(char *) "PaperComment", pol, pol_len);
}

/*
 * PaperComment.{paperId}
 * Comm3c
 */
void
qapla_create_comment_paper_id_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	_qapla_create_review_pid_view_policy(schema, g_sql_pred,
			(char *) "PaperComment", pol, pol_len);
}

void
_qapla_create_table_chair_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *tname, char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// P_ROLE(CHAIR)
	uint64_t role = ROLE_CHAIR|ROLE_ADMIN;
	uint64_t tid = get_schema_table_id(schema, tname);

	char sql_buf[512], *sql_buf_end;
	char *pred[1];
	int pred_len = 0;
	int i;
	for (i = 0; i < 1; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 1; i++)
		free(pred[i]);

	return;
}

/*
 * TI1:
 * TopicInterest.*
 */
void
qapla_create_topic_interest_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "topic_interest");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// P_ROLE(CHAIR|ADMIN) or 
	// (ContactInfo.contactId=TopicInterest.contactId and ContactInfo.email=$k)
	uint64_t role = ROLE_CHAIR|ROLE_ADMIN;
	char *tab_topic_interest = (char *) "TopicInterest";
	uint64_t tid_topic_interest = get_schema_table_id(schema, tab_topic_interest);

	char sql_buf[512], *sql_buf_end;
	char *pred[2];
	int pred_len = 0;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 512);

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_self_topic_interest_pred(pred[1], 1, TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 2, 1);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_topic_interest, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);

	return;
}

/*
 * PaperTopic.{*}
 * PTop1
 */
void
qapla_create_paper_topic_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "PaperTopic");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// P_TAB_USER_PAPER_AUTHOR(PaperTopic) or
	// (P_TAB_PAPER_SUBMITTED(PaperTopic) and P_ROLE(CHAIR|PC|ADMIN))
	uint64_t role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	char *tab_ptopic = (char *) "PaperTopic";
	uint64_t tid_ptopic = get_schema_table_id(schema, tab_ptopic);

	char sql_buf[1024], *sql_buf_end;
	char *pred[2];
	char *tmp_sql_buf[2];
	int pred_len = 0;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	memset(sql_buf, 0, 1024);

	g_sql_pred->create_table_submitted_pred(tmp_sql_buf[0], 2, TTYPE_VARLEN,
			tab_ptopic, tab_ptopic+strlen(tab_ptopic)+1, TTYPE_INTEGER, &time_sub);
	g_sql_pred->create_user_role_email_pred(tmp_sql_buf[1], 2, TTYPE_INTEGER,
			&role, TTYPE_VARIABLE, &sess_var);
	combine_pred(pred[1], &pred_len, tmp_sql_buf, 2, 0);

	g_sql_pred->create_table_paper_author_pred(pred[0], 2, TTYPE_VARLEN, tab_ptopic,
			tab_ptopic+strlen(tab_ptopic)+1, TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 2, 1);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_ptopic,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++) {
		free(pred[i]);
		free(tmp_sql_buf[i]);
	}

	return;
}

/*
 * ActionLog.{*}
 * AL1
 */
void
qapla_create_action_log_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "action_log");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// P_TAB_PAPER_MANAGER(ActionLog) or P_TAB_NONCONFLICT_ROLE(CHAIR|ADMIN, ActionLog)
	uint64_t role, conflict;
	char *tab_alog = (char *) "ActionLog";
	uint64_t tid_alog = get_schema_table_id(schema, tab_alog);

	char sql_buf[2048], *sql_buf_end;
	char *pred[2];
	int pred_len = 0;
	int i;
	for (i = 0; i < 2; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 2048);

	role = ROLE_CHAIR|ROLE_ADMIN;
	conflict = CONFLICT_MAX;
	g_sql_pred->create_table_paper_manager(pred[0], 2, TTYPE_VARLEN, tab_alog,
			tab_alog+strlen(tab_alog)+1, TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_table_nonconflict_role_pred(pred[1], 4, TTYPE_VARLEN,
			tab_alog, tab_alog+strlen(tab_alog)+1, TTYPE_VARIABLE, &sess_var,
			TTYPE_INTEGER, &role, TTYPE_INTEGER, &conflict);
	combine_pred(sql_buf, &pred_len, pred, 2, 1);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, 
			TTYPE_INTEGER, &tid_alog, TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 2; i++)
		free(pred[i]);

	return;
}

/*
 * MailLog.{*}
 * ML1
 */
void
qapla_create_mail_log_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	_qapla_create_table_chair_policy(schema, g_sql_pred,
			(char *) "MailLog", pol, pol_len);
	qapla_policy_t *qp = (qapla_policy_t *) pol;
	sprintf(qp->alias, "mail_log");
}

void
_qapla_create_revrequest_view_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *tname, char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// P_TAB_PAPER_MANAGER(ReviewRequest) or P_ROLE(CHAIR|ADMIN) or
	// P_TAB_REQUESTED(PC|CHAIR|ADMIN, ReviewRequest)
	uint64_t role, conflict;
	uint64_t tid = get_schema_table_id(schema, tname);
	int connector = 0;

	char sql_buf[2048], *sql_buf_end;
	char *pred[4];
	int pred_len = 0;
	int i;
	for (i = 0; i < 4; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 2048);

	role = ROLE_CHAIR|ROLE_ADMIN;
	conflict = CONFLICT_MAX;
	g_sql_pred->create_table_paper_manager(pred[0], 2, TTYPE_VARLEN, tname,
			tname+strlen(tname)+1, TTYPE_VARIABLE, &sess_var);
	g_sql_pred->create_user_role_email_pred(pred[1], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	g_sql_pred->create_table_rreq_pred(pred[2], 3, TTYPE_VARLEN, tname,
			tname+strlen(tname)+1, TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role);

	connector = 1;
	connector |= 1 << 1;
	combine_pred(sql_buf, &pred_len, pred, 3, connector);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 4; i++)
		free(pred[i]);

	return;
}

/*
 * ReviewRequest.{*}
 * RREQ1
 */
void
qapla_create_revrequest_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	_qapla_create_revrequest_view_policy(schema, g_sql_pred,
			(char *) "ReviewRequest", pol, pol_len);
	qapla_policy_t *qp = (qapla_policy_t *) pol;
	sprintf(qp->alias, "review_request");
}

/*
 * PaperReviewRefused.{*}
 * PRR1
 */
void
qapla_create_revrefused_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	_qapla_create_revrequest_view_policy(schema, g_sql_pred,
			(char *) "PaperReviewRefused", pol, pol_len);
	qapla_policy_t *qp = (qapla_policy_t *) pol;
	sprintf(qp->alias, "review_refused");
}

/*
 * PaperReviewPreference.{*}
 * PRP1
 */
void
qapla_create_revpreference_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "review_prefs");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// P_TAB_PAPER_MANAGER(PaperReviewPreference) or
	// P_TAB_NONCONFLICT_ROLE(CHAIR|ADMIN, PaperReviewPreference) or
	// (P_SELF_REVPREF(PC) and P_TAB_NONCONFLICT_ROLE(PC, PaperReviewPreference))
	uint64_t role, conflict;
	char *tab_prp = (char *) "PaperReviewPreference";
	uint64_t tid_prp = get_schema_table_id(schema, tab_prp);

	char sql_buf[2048], *sql_buf_end;
	char *pred[3];
	char *tmp_sql_buf[2];
	int pred_len = 0;
	int connector = 0;
	int i;
	for (i = 0; i < 3; i++) {
		pred[i] = (char *) malloc(1024);
		memset(pred[i], 0, 1024);
	}
	for (i = 0; i < 2; i++) {
		tmp_sql_buf[i] = (char *) malloc(1024);
		memset(tmp_sql_buf[i], 0, 1024);
	}
	memset(sql_buf, 0, 2048);

	g_sql_pred->create_table_paper_manager(pred[0], 2, TTYPE_VARLEN, tab_prp,
			tab_prp+strlen(tab_prp)+1, TTYPE_VARIABLE, &sess_var);
	role = ROLE_CHAIR|ROLE_ADMIN;
	conflict = CONFLICT_MAX;
	g_sql_pred->create_table_nonconflict_role_pred(pred[1], 4, TTYPE_VARLEN,
			tab_prp, tab_prp+strlen(tab_prp)+1, TTYPE_VARIABLE, &sess_var,
			TTYPE_INTEGER, &role, TTYPE_INTEGER, &conflict);

	role = ROLE_PC;
	g_sql_pred->create_table_self_revpref_pred(tmp_sql_buf[0], 2,
			TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role);
	g_sql_pred->create_table_nonconflict_role_pred(tmp_sql_buf[1], 4,
			TTYPE_VARLEN, tab_prp, tab_prp+strlen(tab_prp)+1,
			TTYPE_VARIABLE, &sess_var, TTYPE_INTEGER, &role, TTYPE_INTEGER, &conflict);
	combine_pred(pred[2], &pred_len, tmp_sql_buf, 2, 0);

	connector = 1;
	connector |= (1 << 1);
	combine_pred(sql_buf, &pred_len, pred, 3, connector);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_prp,
			TTYPE_VARLEN, sql_buf, sql_buf_end);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 3; i++)
		free(pred[i]);
	for (i = 0; i < 2; i++)
		free(tmp_sql_buf[i]);

	return;
}

void
qapla_create_paperwatch_policy(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len)
{
	uint16_t num_clauses = 1;

	char *read_dlog_op, *read_sql_op;
	dlog_pi_op_t *dlog_op, *sql_op;
	int op_it = 0;

	qapla_policy_t *qp = (qapla_policy_t *) pol;
	qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
	sprintf(qp->alias, "paper_watch");

	char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
	uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

	// == dlog ==
	// session_is($k)
	read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
	dlog_op = (dlog_pi_op_t *) read_dlog_op;
	dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

	// == sql ==
	// P_ROLE(CHAIR|PC|ADMIN)
	uint64_t role = ROLE_CHAIR|ROLE_PC|ROLE_ADMIN;
	char *tab_watch = (char *) "PaperWatch";
	uint64_t tid_watch = get_schema_table_id(schema, tab_watch);

	char sql_buf[1024], *sql_buf_end;
	char *pred[1];
	int pred_len = 0;
	int i;
	for (i = 0; i < 1; i++) {
		pred[i] = (char *) malloc(512);
		memset(pred[i], 0, 512);
	}
	memset(sql_buf, 0, 1024);

	g_sql_pred->create_user_role_email_pred(pred[0], 2, TTYPE_INTEGER, &role,
			TTYPE_VARIABLE, &sess_var);
	combine_pred(sql_buf, &pred_len, pred, 1, 0);
	sql_buf_end = sql_buf + pred_len + 1;

	read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
	sql_op = (dlog_pi_op_t *) read_sql_op;
	sql_op = create_sql(sql_op, TTYPE_INTEGER, &tid_watch,
			TTYPE_VARLEN, sql_buf, sql_buf_end);	
	qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

	qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

	*pol_len = qapla_policy_size(qp);

	for (i = 0; i < 1; i++)
		free(pred[i]);

	return;
}

// == column list for each hotcrp policy ==

static void
_add_count_sym(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	transform_t *tf;
	char *tname, *cname, *name;

	tname = (char *) "*";
	cname = (char *) "*";
	name = (char *) "*";

	cs = (col_sym_t *) malloc(sizeof(col_sym_t));
	init_col_sym(cs);
	set_sym_field(cs, type, PROJECT);
	tf = init_transform(COUNT);
	list_insert(&cs->xform_list, &tf->xform_listp);
	set_sym_field(cs, db_cid, SPECIAL_CID);
	set_sym_str_field(cs, db_cname, cname);
	set_sym_str_field(cs, db_tname, tname);
	set_sym_str_field(cs, name, name);

	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_contact_keys(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "ContactInfo";

	cname = (char *) "contactId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_contact_info(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "ContactInfo";

	cname = (char *) "firstName";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "lastName";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "unaccentedName";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "affiliation";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "roles";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

}

static void
_add_contact_pc_view_info(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "ContactInfo";

	cname = (char *) "contactTags";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "defaultWatch";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "voicePhoneNumber";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_contact_login_info(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "ContactInfo";

	cname = (char *) "disabled";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "lastLogin";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "creationTime";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_contact_collab_info(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "ContactInfo";

	cname = (char *) "collaborators";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_chair_owner_view_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "ContactInfo";

	cname = (char *) "password";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_contact_private_info(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "ContactInfo";

	cname = (char *) "passwordIsCdb";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "passwordTime";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "data";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "faxPhoneNumber";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_contact_email(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "ContactInfo";

	cname = (char *) "email";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "preferredEmail";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_all_keys(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "paperId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "paperStorageId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "finalPaperStorageId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_keys(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "paperId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_title_abstract(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "title";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "abstract";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_title_abstract_topic(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	transform_t *tf;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "title";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	tf = init_transform(TOPIC);
	list_insert(&cs->xform_list, &tf->xform_listp);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "abstract";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	tf = init_transform(TOPIC);
	list_insert(&cs->xform_list, &tf->xform_listp);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_title_abstract_topic_for_group(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	transform_t *tf;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "title";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT|GROUP);
	tf = init_transform(TOPIC);
	list_insert(&cs->xform_list, &tf->xform_listp);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "abstract";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT|GROUP);
	tf = init_transform(TOPIC);
	list_insert(&cs->xform_list, &tf->xform_listp);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_author(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "authorInformation";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_author_affil(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	transform_t *tf;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "authorInformation";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	tf = init_transform(AFFIL);
	list_insert(&cs->xform_list, &tf->xform_listp);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_author_affil_for_group(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	transform_t *tf;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "authorInformation";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT|GROUP);
	tf = init_transform(AFFIL);
	list_insert(&cs->xform_list, &tf->xform_listp);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_author_country_for_group(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	transform_t *tf;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "authorInformation";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT|GROUP);
	tf = init_transform(COUNTRY);
	list_insert(&cs->xform_list, &tf->xform_listp);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_auth_collab(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "collaborators";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_outcome(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "outcome";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_outcome_for_group(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "outcome";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT|GROUP);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_lead(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "leadContactId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_manager(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "managerContactId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_shepherd(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "shepherdContactId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_time(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "timeSubmitted";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "timeWithdrawn";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_time_for_link_only(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "timeSubmitted";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "timeWithdrawn";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_paper_metadata(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "Paper";

	cname = (char *) "timestamp";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "withdrawReason";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "mimetype";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "size";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "blind";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "sha1";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "paperStorageId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "finalPaperStorageId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "timeFinalSubmitted";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "capVersion";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_pstore_keys(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperStorage";

	cname = (char *) "paperId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "paperStorageId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "originalStorageId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_pstore_paper(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperStorage";

	cname = (char *) "paper";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "timestamp";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "mimetype";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "compression";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "sha1";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "documentType";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "filename";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "infoJson";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "size";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "filterType";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_review_all_keys(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "reviewId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "paperId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "contactId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_review_paper_key(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "paperId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_review_paper_key_for_group(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "paperId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|GROUP|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_review_contact_key_for_group(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "contactId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|GROUP|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

void
_qapla_add_avg_overall_merit_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	transform_t *tf;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "overAllMerit";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	tf = init_transform(AVG);
	list_insert(&cs->xform_list, &tf->xform_listp);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_review_revid_key(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "reviewId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_review_contact_key(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "contactId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_review_status_public(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "reviewRound";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_review_status_private(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "reviewType";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "reviewNotified";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "requestedBy";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	// since only the latest version of the review seems to be stored, it is safe
	// to allow all non-conflicting PC members to query this field. else, each
	// version except the latest should be private to the review author.
	cname = (char *) "reviewEditVersion";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_review_submitted(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "reviewSubmitted";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "reviewNeedsSubmit";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_review_public(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "overAllMerit";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "reviewerQualification";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "novelty";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "technicalMerit";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "strengthOfPaper";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "weaknessOfPaper";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "paperSummary";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "commentsToAuthor";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "commentsToAddress";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "reviewOrdinal";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "suitableForShort";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "grammar";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "reviewWordCount";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "reviewFormat";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "reviewModified";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_review_private(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "commentsToPC";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "interestToCommunity";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "longevity";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "likelyPresentation";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "potential";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "fixability";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "textField7";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "textField8";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "reviewAuthorNotified";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "reviewAuthorSeen";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "timeRequestNotified";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "timeRequested";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "reviewToken";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

void
_qapla_add_review_true_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperReview";

	cname = (char *) "reviewBlind";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_conflict_keys(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperConflict";

	cname = (char *) "paperId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "contactId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_conflict_type(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperConflict";

	cname = (char *) "conflictType";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_comment_id_key(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperComment";

	cname = (char *) "commentId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_comment_contact_key(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperComment";

	cname = (char *) "contactId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_comment_paper_id_key(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperComment";

	cname = (char *) "paperId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_comment_public(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperComment";

	cname = (char *) "comment";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "paperStorageId";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "authorOrdinal";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "commentType";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "commentTags";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "commentRound";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "commentFormat";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_comment_time(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperComment";

	cname = (char *) "timeModified";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "timeNotified";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

static void
_add_comment_private(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperComment";

	cname = (char *) "replyTo";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);

	cname = (char *) "ordinal";
	cs = alloc_init_col_sym_from_schema(schema, tname, cname);
	set_sym_field(cs, type, FILTER|PROJECT);
	list_insert(clist, &cs->col_sym_listp);
}

// C1
void
qapla_create_contact_info_col(db_t *schema, list_t *clist)
{
	_add_contact_info(schema, clist);
}

// C2
void
qapla_create_contact_email_col(db_t *schema, list_t *clist)
{
	_add_contact_email(schema, clist);
}

// C3
void
qapla_create_contact_id_col(db_t *schema, list_t *clist)
{
	_add_contact_keys(schema, clist);
}

void
qapla_create_contact_pc_view_col(db_t *schema, list_t *clist)
{
	// login info should be in chair_owner_view_col, 
	// but added here to minimize hotcrp changes
	_add_contact_login_info(schema, clist);
	_add_contact_pc_view_info(schema, clist);
	_add_contact_collab_info(schema, clist);
}

void
qapla_create_contact_chair_owner_view_col(db_t *schema, list_t *clist)
{
	_add_chair_owner_view_col(schema, clist);
}

void
qapla_create_contact_private_col(db_t *schema, list_t *clist)
{
	_add_contact_private_info(schema, clist);
}

// P1
void
qapla_create_paper_title_col(db_t *schema, list_t *clist)
{
	_add_paper_keys(schema, clist);
	_add_paper_title_abstract(schema, clist);
	_add_paper_time(schema, clist);
}

// P2
void
qapla_create_paper_author_col(db_t *schema, list_t *clist)
{
	_add_paper_author(schema, clist);
}

// P2
void
qapla_create_paper_title_author_col(db_t *schema, list_t *clist)
{
	_add_paper_keys(schema, clist);
	_add_paper_title_abstract(schema, clist);
	_add_paper_time(schema, clist);
	_add_paper_author(schema, clist);
}

void
qapla_create_paper_title_topic_col(db_t *schema, list_t *clist)
{
	_add_paper_keys(schema, clist);
	_add_paper_title_abstract_topic(schema, clist);
	_add_paper_time(schema, clist);
	//_add_paper_time_for_link_only(schema, clist);
	//_add_paper_outcome_for_group(schema, clist);
	//_add_count_sym(schema, clist);
}

void
qapla_create_paper_author_affil_col(db_t *schema, list_t *clist)
{
	_add_paper_author_affil(schema, clist);
	//_add_paper_time_for_link_only(schema, clist);
	//_add_paper_outcome_for_group(schema, clist);
	//_add_count_sym(schema, clist);
}

void
qapla_create_paper_aggr_topic_col(db_t *schema, list_t *clist)
{
	_add_paper_title_abstract_topic_for_group(schema, clist);
	_add_paper_outcome_for_group(schema, clist);
	_add_paper_time_for_link_only(schema, clist);
	_add_count_sym(schema, clist);
}

void
qapla_create_paper_aggr_affil_col(db_t *schema, list_t *clist)
{
	_add_paper_author_affil_for_group(schema, clist);
	_add_paper_time_for_link_only(schema, clist);
	_add_paper_outcome_for_group(schema, clist);
	_add_count_sym(schema, clist);
}

void
qapla_create_paper_aggr_topic_country_col(db_t *schema, list_t *clist)
{
	_add_paper_title_abstract_topic_for_group(schema, clist);
	_add_paper_author_country_for_group(schema, clist);
	_add_paper_outcome_for_group(schema, clist);
	_add_paper_time_for_link_only(schema, clist);
	_add_count_sym(schema, clist);
}

// P3
void
qapla_create_paper_outcome_col(db_t *schema, list_t *clist)
{
	_add_paper_outcome(schema, clist);
}

void
qapla_create_paper_shepherd_col(db_t *schema, list_t *clist)
{
	_add_paper_shepherd(schema, clist);
}

void
qapla_create_aggr_group_outcome_col(db_t *schema, list_t *clist)
{
	_add_paper_outcome_for_group(schema, clist);
	_add_count_sym(schema, clist);
	_add_paper_time_for_link_only(schema, clist);
}

// P4
void
qapla_create_paper_lead_col(db_t *schema, list_t *clist)
{
	_add_paper_lead(schema, clist);
}

void
qapla_create_paper_manager_col(db_t *schema, list_t *clist)
{
	_add_paper_manager(schema, clist);
}

// P5
void
qapla_create_paper_collab_col(db_t *schema, list_t *clist)
{
	_add_paper_auth_collab(schema, clist);
}

void
qapla_create_paper_meta_col(db_t *schema, list_t *clist)
{
	_add_paper_metadata(schema, clist);
}

// PS1
void
qapla_create_pstore_col(db_t *schema, list_t *clist)
{
	_add_pstore_keys(schema, clist);
	_add_pstore_paper(schema, clist);
}

// R1
void
qapla_create_review_for_author_col(db_t *schema, list_t *clist)
{
	_add_review_revid_key(schema, clist);
	_add_review_paper_key(schema, clist);
	// make reviewsubmitted accessible to authors for now
	_add_review_submitted(schema, clist);
	_add_review_public(schema, clist);
	_add_review_status_public(schema, clist);
}

void
qapla_create_review_private_col(db_t *schema, list_t *clist)
{
	_add_review_contact_key(schema, clist);
	_add_review_status_private(schema, clist);
	_add_review_private(schema, clist);
}

void
qapla_create_aggr_rev_status_by_pid_col(db_t *schema, list_t *clist)
{
	_add_review_submitted(schema, clist);
	_add_review_paper_key_for_group(schema, clist);
	_add_count_sym(schema, clist);
}

void
qapla_create_aggr_rev_status_by_cid_col(db_t *schema, list_t *clist)
{
	_add_review_submitted(schema, clist);
	_add_review_contact_key_for_group(schema, clist);
	_add_count_sym(schema, clist);
}

void
qapla_create_avg_overall_merit_pc_col(db_t *schema, list_t *clist)
{
	_add_review_contact_key_for_group(schema, clist);
	_qapla_add_avg_overall_merit_col(schema, clist);
}

void
qapla_create_review_paper_aggr_topic_col(db_t *schema, list_t *clist)
{
	_add_paper_keys(schema, clist);
	_add_review_paper_key(schema, clist);
	_add_paper_time_for_link_only(schema, clist);
	_add_paper_title_abstract_topic_for_group(schema, clist);
	_add_paper_outcome_for_group(schema, clist);
	_add_count_sym(schema, clist);
}

void
qapla_create_review_paper_aggr_affil_col(db_t *schema, list_t *clist)
{
	_add_paper_keys(schema, clist);
	_add_review_paper_key(schema, clist);
	_add_paper_time_for_link_only(schema, clist);
	_add_paper_author_affil_for_group(schema, clist);
	_add_paper_outcome_for_group(schema, clist);
	_add_count_sym(schema, clist);
}

void
qapla_create_avg_overall_merit_topic_col(db_t *schema, list_t *clist)
{
	_add_paper_keys(schema, clist);
	_add_review_paper_key(schema, clist);
	_add_paper_time_for_link_only(schema, clist);
	_add_paper_title_abstract_topic_for_group(schema, clist);
	_add_paper_outcome_for_group(schema, clist);
	_qapla_add_avg_overall_merit_col(schema, clist);
}

void
qapla_create_avg_overall_merit_affil_col(db_t *schema, list_t *clist)
{
	_add_paper_keys(schema, clist);
	_add_review_paper_key(schema, clist);
	_add_paper_time_for_link_only(schema, clist);
	_add_paper_author_affil_for_group(schema, clist);
	_add_paper_outcome_for_group(schema, clist);
	_qapla_add_avg_overall_merit_col(schema, clist);
}

void
qapla_create_conflict_col(db_t *schema, list_t *clist)
{
	_add_conflict_keys(schema, clist);
	_add_conflict_type(schema, clist);
}

void
qapla_create_comment_for_author_col(db_t *schema, list_t *clist)
{
	_add_comment_id_key(schema, clist);
	_add_comment_public(schema, clist);
	_add_comment_paper_id_key(schema, clist);
	_add_comment_time(schema, clist);
	// temporarily put contact key in public, to check special query eval
	//_add_comment_contact_key(schema, clist);
}

void
qapla_create_private_comment_col(db_t *schema, list_t *clist)
{
	// temporarily put contact key in public, to check special query eval
	_add_comment_contact_key(schema, clist);
	_add_comment_private(schema, clist);
}

void
qapla_create_comment_id_col(db_t *schema, list_t *clist)
{
	_add_comment_id_key(schema, clist);
}

void
qapla_create_comment_contact_id_col(db_t *schema, list_t *clist)
{
	_add_comment_contact_key(schema, clist);
}

void
qapla_create_comment_paper_id_col(db_t *schema, list_t *clist)
{
	_add_comment_paper_id_key(schema, clist);
}

void
qapla_create_review_rating_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "ReviewRating";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_papertags_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperTag";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_paper_options_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;

	tname = (char *) "PaperOption";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_topic_interest_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "TopicInterest";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_paper_topic_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "PaperTopic";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_action_log_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "ActionLog";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_mail_log_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "MailLog";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_revrequest_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "ReviewRequest";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_revrefused_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "PaperReviewRefused";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_revpreference_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "PaperReviewPreference";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_settings_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "Settings";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_capability_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "Capability";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_formula_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "Formula";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_paperwatch_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "PaperWatch";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_topicarea_col(db_t *schema, list_t *clist)
{
	col_sym_t *cs;
	char *tname, *cname;
	tname = (char *) "TopicArea";
	int tid = get_schema_table_id(schema, tname);
	int num_col = get_schema_num_col_in_table_id(schema, tid);
	int i;
	for (i = 0; i < num_col; i++) {
		cname = get_schema_col_name_in_table_id(schema, tid, i);
		cs = alloc_init_col_sym_from_schema(schema, tname, cname);
		list_insert(clist, &cs->col_sym_listp);
	}
}

void
qapla_create_table_list(db_t *schema, char *tname, list_t *clist)
{
	col_sym_t *tab_cs;
	tab_cs = alloc_init_col_sym_from_schema(schema, tname, NULL);
	set_sym_field(tab_cs, db_cid, sym_field(tab_cs, db_tid));
	list_insert(clist, &tab_cs->col_sym_listp);
}

create_policy_fn_t qapla_hotcrp_cpfn = {
	_qapla_create_sql_false_policy,
	_qapla_create_sql_true_policy,
	qapla_create_contact_info_policy,
	qapla_create_contact_email_policy,
	qapla_create_contact_id_policy,
	qapla_create_contact_pc_view_policy,
	qapla_create_contact_private_policy,
	qapla_create_contact_chair_owner_view_policy,
	qapla_create_paper_title_policy,
	qapla_create_paper_author_policy,
	qapla_create_paper_title_author_policy,
	qapla_create_paper_title_topic_policy,
	qapla_create_paper_author_affil_policy,
	qapla_create_paper_outcome_policy,
	qapla_create_paper_shepherd_policy,
	qapla_create_aggr_group_outcome_policy,
	qapla_create_paper_lead_policy,
	qapla_create_paper_manager_policy,
	qapla_create_paper_meta_policy,
	qapla_create_paper_aggr_topic_policy,
	qapla_create_paper_aggr_affil_policy,
	qapla_create_paper_aggr_topic_country_policy,
	qapla_create_pstore_policy,
	qapla_create_review_for_author_policy,
	qapla_create_review_private_policy,
	qapla_create_aggr_review_status_policy,
	qapla_create_avg_overall_merit_pc_policy,
	_qapla_create_review_paper_analytics_policy,
	qapla_create_review_paper_aggr_topic_policy,
	qapla_create_review_paper_aggr_affil_policy,
	qapla_create_review_rating_policy,
	qapla_create_conflict_policy,
	qapla_create_papertags_policy,
	qapla_create_paper_options_policy,
	qapla_create_comment_for_author_policy,
	qapla_create_private_comment_policy,
	qapla_create_comment_id_or_contact_id_policy,
	qapla_create_comment_paper_id_policy,
	qapla_create_topic_interest_policy,
	qapla_create_paper_topic_policy,
	qapla_create_action_log_policy,
	qapla_create_mail_log_policy,
	qapla_create_revrequest_policy,
	qapla_create_revrefused_policy,
	qapla_create_revpreference_policy,
	qapla_create_paperwatch_policy,
};

create_policy_col_fn_t qapla_hotcrp_cpcfn = {
	qapla_create_contact_info_col,
	qapla_create_contact_email_col,
	qapla_create_contact_id_col,
	qapla_create_contact_pc_view_col,
	qapla_create_contact_private_col,
	qapla_create_contact_chair_owner_view_col,
	qapla_create_paper_title_col,
	qapla_create_paper_author_col,
	qapla_create_paper_title_author_col,
	qapla_create_paper_title_topic_col,
	qapla_create_paper_author_affil_col,
	qapla_create_paper_outcome_col,
	qapla_create_paper_shepherd_col,
	qapla_create_aggr_group_outcome_col,
	qapla_create_paper_lead_col,
	qapla_create_paper_manager_col,
	qapla_create_paper_collab_col,
	qapla_create_paper_meta_col,
	qapla_create_paper_aggr_topic_col,
	qapla_create_paper_aggr_affil_col,
	qapla_create_paper_aggr_topic_country_col,
	qapla_create_pstore_col,
	qapla_create_review_for_author_col,
	qapla_create_review_private_col,
	_qapla_add_review_true_col,
	qapla_create_aggr_rev_status_by_pid_col,
	qapla_create_aggr_rev_status_by_cid_col,
	qapla_create_avg_overall_merit_pc_col,
	_qapla_add_avg_overall_merit_col,
	qapla_create_review_paper_aggr_topic_col,
	qapla_create_review_paper_aggr_affil_col,
	qapla_create_avg_overall_merit_topic_col,
	qapla_create_avg_overall_merit_affil_col,
	qapla_create_review_rating_col,
	qapla_create_conflict_col,
	qapla_create_papertags_col,
	qapla_create_paper_options_col,
	qapla_create_comment_for_author_col,
	qapla_create_private_comment_col,
	qapla_create_comment_id_col,
	qapla_create_comment_contact_id_col,
	qapla_create_comment_paper_id_col,
	qapla_create_topic_interest_col,
	qapla_create_paper_topic_col,
	qapla_create_action_log_col,
	qapla_create_mail_log_col,
	qapla_create_revrequest_col,
	qapla_create_revrefused_col,
	qapla_create_revpreference_col,
	qapla_create_settings_col,
	qapla_create_capability_col,
	qapla_create_formula_col,
	qapla_create_paperwatch_col,
	qapla_create_topicarea_col,
	qapla_create_table_list,
};

