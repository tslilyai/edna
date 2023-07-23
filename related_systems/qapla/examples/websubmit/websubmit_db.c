#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "websubmit_db.h"
#include "websubmit_pol_db.h"
#include "common/qapla_policy.h"

extern sql_pred_t *g_sql_pred;

int
websubmit_load_schema(db_t *db)
{
	if (!db)
		return -1;

	int ret = 0, tid = 0, cid = 0;
	char t_buf[MAX_NAME_LEN];
	memset(t_buf, 0, MAX_NAME_LEN);

	int i;
	sprintf(t_buf, "%s", "users");
	ret |= add_col_in_table(db, (const char *) t_buf, "apikey", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "email", &cid);
	ret |= add_col_in_table_id(db, tid, "is_admin", &cid);
    // XXX added for disguises
	ret |= add_col_in_table_id(db, tid, "is_anon", &cid);
	ret |= add_col_in_table_id(db, tid, "owner", &cid);
	ret |= add_col_in_table_id(db, tid, "is_deleted", &cid);

	sprintf(t_buf, "%s", "lectures");
	ret |= add_col_in_table(db, (const char *) t_buf, "id", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "label", &cid);

	sprintf(t_buf, "%s", "questions");
	ret |= add_col_in_table(db, (const char *) t_buf, "lec", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "q", &cid);
	ret |= add_col_in_table_id(db, tid, "question", &cid);

	sprintf(t_buf, "%s", "answers");
	ret |= add_col_in_table(db, (const char *) t_buf, "email", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "lec", &cid);
	ret |= add_col_in_table_id(db, tid, "q", &cid);
	ret |= add_col_in_table_id(db, tid, "answer", &cid);
	ret |= add_col_in_table_id(db, tid, "submitted_at", &cid);

    // TODO support for views?
    // CREATE VIEW lec_qcount as SELECT questions.lec, COUNT(questions.q) AS qcount FROM questions GROUP BY questions.lec;
    
#if DEBUG
	printf("loaded websubmit schema!\n");
#endif
	return ret;
}

void
websubmit_load_standard_policies(db_t *schema, metadata_t *qm,
		void *sql_pred_fns, void *pol_fns, void *col_fns)
{
	uint16_t pid;
	int pol_len = 0;
	qapla_policy_t qp_buf, *qpp;
	qpp = &qp_buf;
	char *qp = (char *) qpp;

	list_t cs_list;
	list_init(&cs_list);

	cluster_t *c;
	pvec_t *c_pv;
	int cluster_id = 0;
	int c_pid, ret;

	qapla_policy_pool_t *qpool = metadata_get_policy_pool(qm);

	create_policy_fn_t *g_cpfn = (create_policy_fn_t *) pol_fns;
	create_policy_col_fn_t *g_cpcfn = (create_policy_col_fn_t *) col_fns;
	sql_pred_t *g_sql_pred = (sql_pred_t *) sql_pred_fns;

	// cluster 1 (OR)
	c = (cluster_t *) malloc(sizeof(cluster_t));
	cluster_id = metadata_get_next_policy_cluster_id(qm);
	init_cluster(c, cluster_id, OP_OR, C_NON_AGGR);
	c_pv = cluster_get_pvec(c, PVEC_LINK);

	// == policy for answers
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	//g_cpfn->create_sql_true_pol(schema, (char *) "answers", qp, &pol_len);
	g_cpfn->create_answer_pol(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->answer_cols(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
    add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	
	// == policy for users
   	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	//g_cpfn->create_sql_true_pol(schema, (char *) "users", qp, &pol_len);
	g_cpfn->create_user_pol(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->user_cols(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
    add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	
	// == policy for user extra cols
	// XXX lyt: same as user policy for now
   	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	//g_cpfn->create_sql_true_pol(schema, (char *) "users", qp, &pol_len);
	//g_cpfn->create_user_extra_pol(schema, g_sql_pred, qp, &pol_len);
	g_cpfn->create_user_pol(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->user_extra_cols(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
    add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	
	// == policy for lectures
   	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_sql_true_pol(schema, (char *) "lectures", qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->lecture_cols(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
    add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	
	// == policy for questions
   	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_sql_true_pol(schema, (char *) "questions", qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->question_cols(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
    add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

    // done adding policies

	metadata_add_policy_cluster(qm, c);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);

#if DEBUG
	printf("TOTAL POLICIES: %d\n", get_num_policy_in_pool(qpool));
#endif
}
