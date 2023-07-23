#ifndef __WEBSUBMIT_POL_DB_H__
#define __WEBSUBMIT_POL_DB_H__

#include "utils/list.h"
#include "common/db.h"

#ifdef __cplusplus
extern "C" {
#endif

// == predicates used for policies ==
typedef struct sql_pred {
    char *(*answer_pred)(char *op, int num_args, ...);
    char *(*user_pred)(char *op, int num_args, ...);
} sql_pred_t;

extern sql_pred_t websubmit_mysql_pred;

// == column list for each websubmit policy ==
typedef struct create_policy_col_fn {
		void (*user_cols)(db_t *schema, list_t *clist);
		void (*user_extra_cols)(db_t *schema, list_t *clist);
		void (*answer_cols)(db_t *schema, list_t *clist);
		void (*lecture_cols)(db_t *schema, list_t *clist);
		void (*question_cols)(db_t *schema, list_t *clist);
} create_policy_col_fn_t;

// == websubmit policies ==
typedef struct create_policy_fn {
	void (*create_sql_true_pol)(db_t *schema, char *table,
		char *pol, int *pol_len);
	void (*create_answer_pol)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
    void (*create_user_pol)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
    void (*create_user_extra_pol)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
} create_policy_fn_t;

extern create_policy_fn_t qapla_websubmit_cpfn;
extern create_policy_fn_t *g_cpfn;

extern create_policy_col_fn_t qapla_websubmit_cpcfn;
extern create_policy_col_fn_t *g_cpcfn;

#ifdef __cplusplus
}
#endif

#endif /* __WEBSUBMIT_POL_DB_H__ */
