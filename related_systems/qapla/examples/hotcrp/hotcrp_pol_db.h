#ifndef __QAPLA_POL_DB_H__
#define __QAPLA_POL_DB_H__

#include "utils/list.h"
#include "common/db.h"
#include "hotcrp_sql_pred.h"

#ifdef __cplusplus
extern "C" {
#endif

// == hotcrp policies ==
typedef struct create_policy_fn {
	void (*_create_sql_false_policy)(db_t *schema, char *tname, char *pol, int *pol_len);
	void (*_create_sql_true_policy)(db_t *schema, sql_pred_t *g_sql_pred,
			char *tname, char *pol, int *pol_len);
	void (*create_contact_info_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_contact_email_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_contact_id_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_contact_pc_view_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_contact_private_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_contact_chair_owner_view_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);

	void (*create_paper_title_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_author_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_title_author_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_title_topic_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_author_affil_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_outcome_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_shepherd_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_aggr_group_outcome_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_lead_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_manager_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_meta_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_aggr_topic_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_aggr_affil_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_aggr_topic_country_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);

	void (*create_pstore_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);

	void (*create_review_for_author_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_review_private_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_aggr_review_status_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_avg_overall_merit_pc_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);

	void (*_create_review_paper_analytics_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_review_paper_aggr_topic_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_review_paper_aggr_affil_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);

	void (*create_review_rating_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);

	void (*create_conflict_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_papertags_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_options_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);

	void (*create_comment_for_author_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_private_comment_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_comment_id_or_contact_id_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_comment_paper_id_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);

	void (*create_topic_interest_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paper_topic_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_action_log_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_mail_log_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_revrequest_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_revrefused_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_revpreference_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
	void (*create_paperwatch_policy)(db_t *schema, sql_pred_t *g_sql_pred,
		char *pol, int *pol_len);
} create_policy_fn_t;

extern create_policy_fn_t qapla_hotcrp_cpfn;
extern create_policy_fn_t *g_cpfn;

// == column list for each hotcrp policy ==
typedef struct create_policy_col_fn {
	void (*create_contact_info_col)(db_t *schema, list_t *clist);
	void (*create_contact_email_col)(db_t *schema, list_t *clist);
	void (*create_contact_id_col)(db_t *schema, list_t *clist);
	void (*create_contact_pc_view_col)(db_t *schema, list_t *clist);
	void (*create_contact_private_col)(db_t *schema, list_t *clist);
	void (*create_contact_chair_owner_view_col)(db_t *schema, list_t *clist);

	void (*create_paper_title_col)(db_t *schema, list_t *clist);
	void (*create_paper_author_col)(db_t *schema, list_t *clist);
	void (*create_paper_title_author_col)(db_t *schema, list_t *clist);
	void (*create_paper_title_topic_col)(db_t *schema, list_t *clist);
	void (*create_paper_author_affil_col)(db_t *schema, list_t *clist);
	void (*create_paper_outcome_col)(db_t *schema, list_t *clist);
	void (*create_paper_shepherd_col)(db_t *schema, list_t *clist);
	void (*create_aggr_group_outcome_col)(db_t *schema, list_t *clist);
	void (*create_paper_lead_col)(db_t *schema, list_t *clist);
	void (*create_paper_manager_col)(db_t *schema, list_t *clist);
	void (*create_paper_collab_col)(db_t *schema, list_t *clist);
	void (*create_paper_meta_col)(db_t *schema, list_t *clist);
	void (*create_paper_aggr_topic_col)(db_t *schema, list_t *clist);
	void (*create_paper_aggr_affil_col)(db_t *schema, list_t *clist);
	void (*create_paper_aggr_topic_country_col)(db_t *schema, list_t *clist);

	void (*create_pstore_col)(db_t *schema, list_t *clist);

	void (*create_review_for_author_col)(db_t *schema, list_t *clist);
	void (*create_review_private_col)(db_t *schema, list_t *clist);
	void (*_add_review_true_col)(db_t *schema, list_t *clist);

	void (*create_aggr_rev_status_by_pid_col)(db_t *schema, list_t *clist);
	void (*create_aggr_rev_status_by_cid_col)(db_t *schema, list_t *clist);
	void (*create_avg_overall_merit_pc_col)(db_t *schema, list_t *clist);
	void (*_add_avg_overall_merit_col)(db_t *schema, list_t *clist);

	void (*create_review_paper_aggr_topic_col)(db_t *schema, list_t *clist);
	void (*create_review_paper_aggr_affil_col)(db_t *schema, list_t *clist);
	void (*create_avg_overall_merit_topic_col)(db_t *schema, list_t *clist);
	void (*create_avg_overall_merit_affil_col)(db_t *schema, list_t *clist);

	void (*create_review_rating_col)(db_t *schema, list_t *clist);

	void (*create_conflict_col)(db_t *schema, list_t *clist);
	void (*create_papertags_col)(db_t *schema, list_t *clist);
	void (*create_paper_options_col)(db_t *schema, list_t *clist);

	void (*create_comment_for_author_col)(db_t *schema, list_t *clist);
	void (*create_private_comment_col)(db_t *schema, list_t *clist);
	void (*create_comment_id_col)(db_t *schema, list_t *clist);
	void (*create_comment_contact_id_col)(db_t *schema, list_t *clist);
	void (*create_comment_paper_id_col)(db_t *schema, list_t *clist);

	void (*create_topic_interest_col)(db_t *schema, list_t *clist);
	void (*create_paper_topic_col)(db_t *schema, list_t *clist);
	void (*create_action_log_col)(db_t *schema, list_t *clist);
	void (*create_mail_log_col)(db_t *schema, list_t *clist);
	void (*create_revrequest_col)(db_t *schema, list_t *clist);
	void (*create_revrefused_col)(db_t *schema, list_t *clist);
	void (*create_revpreference_col)(db_t *schema, list_t *clist);
	void (*create_settings_col)(db_t *schema, list_t *clist);
	void (*create_capability_col)(db_t *schema, list_t *clist);
	void (*create_formula_col)(db_t *schema, list_t *clist);
	void (*create_paperwatch_col)(db_t *schema, list_t *clist);
	void (*create_topicarea_col)(db_t *schema, list_t *clist);
	void (*create_table_list)(db_t *schema, char *tname, list_t *clist);
} create_policy_col_fn_t;

extern create_policy_col_fn_t qapla_hotcrp_cpcfn;
extern create_policy_col_fn_t *g_cpcfn;

#ifdef __cplusplus
}
#endif

#endif /* __QAPLA_POL_DB_H__ */
