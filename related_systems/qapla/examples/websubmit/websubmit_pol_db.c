#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <ctype.h>

#include "utils/list.h"
#include "common/qapla_policy.h"
#include "common/dlog_pi_ds.h"
#include "common/col_sym.h"
#include "common/db.h"
#include "common/tuple.h"
#include "common/config.h"
#include "websubmit_pol_db.h"
#include "policyapi/dlog_pred.h"
#include "policyapi/sql_pred.h"

/*********************************************************************
 *             Columns 
 *********************************************************************/
// we allow all columns to be used in SELECT and WHERE expressions
    static void
user_extra_cols(db_t *schema, list_t *clist)
{
    col_sym_t *cs;
    char *tname, *cname;

    tname = (char *) "users";

    cname = (char *) "is_deleted";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);
}

    static void
question_cols(db_t *schema, list_t *clist)
{
    col_sym_t *cs;
    char *tname, *cname;

    tname = (char *) "questions";

    cname = (char *) "lec";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);

    cname = (char *) "q";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);

    cname = (char *) "question";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);
}

    static void
lecture_cols(db_t *schema, list_t *clist)
{
    col_sym_t *cs;
    char *tname, *cname;

    tname = (char *) "lectures";

    cname = (char *) "id";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);

    cname = (char *) "label";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);
}


    static void
user_cols(db_t *schema, list_t *clist)
{
    col_sym_t *cs;
    char *tname, *cname;

    tname = (char *) "users";

    cname = (char *) "email";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);

    cname = (char *) "apikey";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);

    cname = (char *) "is_admin";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);

    // these need to be queryable by the admin because we select upon in the where clause
    cname = (char *) "is_anon";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);

    cname = (char *) "owner";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);
}

    static void
answer_cols(db_t *schema, list_t *clist)
{
    col_sym_t *cs;
    char *tname, *cname;

    tname = (char *) "answers";

    cname = (char *) "email";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);

    cname = (char *) "lec";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);

    cname = (char *) "q";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);

    cname = (char *) "answer";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);

    cname = (char *) "submitted_at";
    cs = alloc_init_col_sym_from_schema(schema, tname, cname);
    set_sym_field(cs, type, FILTER|PROJECT);
    list_insert(clist, &cs->col_sym_listp);
}

create_policy_col_fn_t qapla_websubmit_cpcfn = {
    user_cols,
    user_extra_cols,
    answer_cols,
    lecture_cols,
    question_cols,
};

/***************************************************************
 *              PREDICATES
 ***************************************************************/
// only return answers if either:
// - the author is the session user or a pp of the user
// - session user is admin and author is not deleted
    char *
answer_pred(char *op, int num_args, ...)
{
    char pbuf[2048];
    memset(pbuf, 0, 2048);
    va_list ap;

#if DEBUG
    printf("Number args user pred: %d\n", num_args);
#endif

   va_start(ap, num_args);
    uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
    uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

    int i;
    for (i = 0; i < num_args; i++) {
        type[i] = va_arg(ap, uint32_t);
        val[i] = va_arg(ap, uint8_t *);
    }

    char email_buf[512];
    memset(email_buf, 0, 512);
    if (type[0] == TTYPE_VARIABLE)
        sprintf(email_buf, "\':%d\'", *(uint16_t *) val[0]);
    else
        sprintf(email_buf, "\'%s\'", (char *) val[0]);

    sprintf(pbuf, "exists(select 1 from answers a join users u FORCE INDEX (owner, is_deleted) on "
            "(a.email=u.email) where "
	    "(u.email=%s OR u.owner=%s) " // get answers belong to this user or this user's pps
	    "OR (%s='malte@cs.brown.edu' AND u.is_deleted=0)) ", // admin can get all non-deleted answers 
	    email_buf, email_buf, email_buf);
    op = add_pred(op, pbuf, strlen(pbuf));

    va_end(ap);
    free(type);
    free(val);
    return op;
}

// USER POLICY: 
// - user is the session user or a pp of the session user
// - session user is admin and use is not deleted
    char *
user_pred(char *op, int num_args, ...)
{
    char pbuf[2048];
    memset(pbuf, 0, 2048);
    va_list ap;

    va_start(ap, num_args);
    uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
    uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

#if DEBUG
    printf("Number args user pred: %d\n", num_args);
#endif

    type[0] = va_arg(ap, uint32_t);
    val[0] = va_arg(ap, uint8_t *);

    char email_buf[512];
    memset(email_buf, 0, 512);
    if (type[0] == TTYPE_VARIABLE)
        sprintf(email_buf, "\':%d\'", *(uint16_t *) val[0]);
    else
        sprintf(email_buf, "\'%s\'", (char *) val[0]);

    sprintf(pbuf, "exists(select 1 from users u FORCE INDEX (is_deleted) "
		    "where u.email=%s OR u.owner=%s " // user or user pp
	    		"OR (%s='malte@cs.brown.edu' AND u.is_deleted=0)) ", // admin can get all non-deleted users
	    email_buf, email_buf, email_buf);
    op = add_pred(op, pbuf, strlen(pbuf));

    va_end(ap);
    free(type);
    free(val);
    return op;
}

sql_pred_t websubmit_mysql_pred = {
    answer_pred,
    user_pred,
};

/***************************************************************
 *             POLICIES 
 ***************************************************************/
    static char *
create_sql_true_op(db_t *schema, 
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
create_sql_true_pol(db_t *schema, 
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
    sql_op = (dlog_pi_op_t *) create_sql_true_op(schema, sql_op, tname);
    qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

#if DEBUG
    printf("true pol sql: %s\n", (char *) sql_op);
#endif

    qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

    *pol_len = qapla_policy_size(qp);
}

    void
create_user_extra_pol(db_t *schema, sql_pred_t *g_sql_pred,
        char *pol, int *pol_len)
{
    // just false
    uint16_t num_clauses = 1;

    int op_it = 0;

    char *read_dlog_op, *read_sql_op;
    dlog_pi_op_t *dlog_op, *sql_op;

    qapla_policy_t *qp = (qapla_policy_t *) pol;
    qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);

    char *read_perm = qapla_start_perm(qp, QP_PERM_READ);

    char *false_str = (char *) "1=0";
    uint64_t tid = get_schema_table_id(schema, "users");

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

    void
create_user_pol(db_t *schema, sql_pred_t *g_sql_pred,
        char *pol, int *pol_len)
{
    uint16_t num_clauses = 1;

    char *read_dlog_op, *read_sql_op;
    dlog_pi_op_t *dlog_op, *sql_op;
    int op_it = 0;

    qapla_policy_t *qp = (qapla_policy_t *) pol;
    qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
    sprintf(qp->alias, "user");

    char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
    uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

#if DEBUG
    printf("creating user pol 1\n");
#endif

    // == dlog 1 ==
    // session_is($k)
    read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
    dlog_op = (dlog_pi_op_t *) read_dlog_op;
    dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
    qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

#if DEBUG
    printf("creating user pol dlog1\n");
#endif

    // ==
    char *tab_users = (char *) "users";
    uint64_t tid_users = get_schema_table_id(schema, tab_users);

    int pred_len = 0;
    char *pred[num_clauses];
    char sql_buf[1024], *sql_buf_end;
    int i;
    for (i = 0; i < 2; i++)
        pred[i] = (char *) malloc(1024);

    // == sql 1 ==
    op_it = 0;
    for (i = 0; i < num_clauses; i++)
        memset(pred[i], 0, 1024);

    // XXX this assumes that sess_var is the email address
    g_sql_pred->user_pred(pred[0], 1, TTYPE_VARIABLE, &sess_var);

    memset(sql_buf, 0, 1024);
    combine_pred(sql_buf, &pred_len, &pred[0], 1, 0);
    sql_buf_end = sql_buf + pred_len + 1;

    read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
    sql_op = (dlog_pi_op_t *) read_sql_op;
    sql_op = create_sql(sql_op,
            TTYPE_INTEGER, &tid_users, TTYPE_VARLEN, sql_buf, sql_buf_end);
    qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

#if DEBUG
    printf("tid users: %ld\n", tid_users);
    printf("sql_buf: %s\n", sql_buf);
#endif

    // ==
    qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

    *pol_len = qapla_policy_size(qp);

    for (i = 0; i < num_clauses; i++)
        free(pred[i]);

    return;
}

    void
create_answer_pol(db_t *schema, sql_pred_t *g_sql_pred,
        char *pol, int *pol_len)
{
    uint16_t num_clauses = 1;

    char *read_dlog_op, *read_sql_op;
    dlog_pi_op_t *dlog_op, *sql_op;
    int op_it = 0;

    qapla_policy_t *qp = (qapla_policy_t *) pol;
    qapla_set_perm_clauses(qp, QP_PERM_READ, num_clauses);
    sprintf(qp->alias, "gdpr_answer");

    char *read_perm = qapla_start_perm(qp, QP_PERM_READ);
    uint16_t sess_var = qapla_get_next_var_id(qp, QP_PERM_READ);

    // == dlog 1 ==
    // session_is($k)
    read_dlog_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it);
    dlog_op = (dlog_pi_op_t *) read_dlog_op;
    dlog_op = create_session_is(dlog_op, TTYPE_VARIABLE, &sess_var);
    qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_DLOG, op_it, (char *) dlog_op);

    // ==
    char *tab_answers = (char *) "answers";
    uint64_t tid_answers = get_schema_table_id(schema, tab_answers);

    int pred_len = 0;
    char *pred[num_clauses];
    char sql_buf[1024], *sql_buf_end;
    int i;
    for (i = 0; i < num_clauses; i++)
        pred[i] = (char *) malloc(1024);

    // == sql 1 ==
    op_it = 0;
    for (i = 0; i < num_clauses; i++)
        memset(pred[i], 0, 1024);

    // XXX this assumes that sess_var is the email address
    g_sql_pred->answer_pred(pred[0], 1, TTYPE_VARIABLE, &sess_var);

    memset(sql_buf, 0, 1024);
    combine_pred(sql_buf, &pred_len, &pred[0], 1, 0);
    sql_buf_end = sql_buf + pred_len + 1;

    read_sql_op = qapla_start_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it);
    sql_op = (dlog_pi_op_t *) read_sql_op;
    sql_op = create_sql(sql_op,
            TTYPE_INTEGER, &tid_answers, TTYPE_VARLEN, sql_buf, sql_buf_end);
    qapla_end_perm_clause(qp, QP_PERM_READ, CTYPE_SQL, op_it, (char *) sql_op);

#if DEBUG
    printf("tid answers: %ld\n", tid_answers);
    printf("sql_buf: %s\n", sql_buf);
#endif

    // ==
    qapla_end_perm(qp, QP_PERM_READ, (char *) sql_op);

    *pol_len = qapla_policy_size(qp);

    for (i = 0; i < num_clauses; i++)
        free(pred[i]);

    return;
}

create_policy_fn_t qapla_websubmit_cpfn = {
    create_sql_true_pol,
    create_answer_pol,
    create_user_pol,
    create_user_extra_pol,
};
