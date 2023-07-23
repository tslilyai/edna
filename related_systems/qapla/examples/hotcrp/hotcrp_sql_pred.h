#ifndef __HOTCRP_SQL_PRED_H__
#define __HOTCRP_SQL_PRED_H__

// constants as defined in hotcrp application
#define ROLE_PC	1
#define ROLE_ADMIN	2
#define ROLE_CHAIR	4
#define ROLE_PCLIKE	15
#define ROLE_AUTHOR	16
#define ROLE_REVIEWER	32
#define ROLE_ANALYST	64 // to add in schema

#define COMMENTTYPE_DRAFT 1
#define COMMENTTYPE_PCONLY 0x10000
#define COMMENTTYPE_AUTHOR 0x30000

// constants as defined in hotcrp application
#define CONFLICT_AUTHORMARK 2
#define CONFLICT_MAXAUTHORMARK 7
#define CONFLICT_CHAIRMARK 8
#define CONFLICT_AUTHOR	9
#define CONFLICT_CONTACTAUTHOR 10
#define CONFLICT_MAX 11

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct sql_pred {
		char *(*create_user_role_email_pred)(char *op, int num_args, ...);
		char *(*create_data_role_pred)(char *op, int num_args, ...);
		char *(*create_data_contact_email_pred)(char *op, int num_args, ...);
		char *(*create_contact_conflict_pred)(char *op, int num_args, ...);
		char *(*create_accepted_paper_auth_pred)(char *op, int num_args, ...);
		char *(*create_contact_paper_sheph_pred)(char *op, int num_args, ...);
		char *(*create_table_paper_author_pred)(char *op, int num_args, ...);
		char *(*create_table_paper_manager)(char *op, int num_args, ...);
		char *(*create_rrating_paper_manager)(char *op, int num_args, ...);
		char *(*create_paper_submitted_pred)(char *op, int num_args, ...);
		char *(*create_table_submitted_pred)(char *op, int num_args, ...);
		char *(*create_paper_accepted_pred)(char *op, int num_args, ...);
		//char *(*create_table_accepted_pred)(char *op, int num_args, ...);
		char *(*create_table_nonconflict_role_pred)(char *op, int num_args, ...);
		char *(*create_rrating_nonconflict_role_pred)(char *op, int num_args, ...);
		char *(*create_table_rreq_pred)(char *op, int num_args, ...);
		char *(*create_table_self_revpref_pred)(char *op, int num_args, ...);
		char *(*create_is_review_submitted_pred)(char *op, int num_args, ...);
		char *(*create_review_requester_pred)(char *op, int num_args, ...);
		char *(*create_table_submitted_review_pred)(char *op, int num_args, ...);
		char *(*create_rrating_submitted_review_pred)(char *op, int num_args, ...);
		char *(*create_comment_compare_pred)(char *op, int num_args, ...);
		char *(*create_comment_ordinal_pred)(char *op, int num_args, ...);
		char *(*create_pc_role_conflict_pred)(char *op, int num_args, ...);
		char *(*create_pc_conflict_view_pred)(char *op, int num_args, ...);
		char *(*create_author_conflict_pred)(char *op, int num_args, ...);
		char *(*create_self_topic_interest_pred)(char *op, int num_args, ...);
		char *(*create_time_compare_pred)(char *op, int num_args, ...);
		char *(*create_time_presub_pred)(char *op, int num_args, ...);
		char *(*create_time_sub_pred)(char *op, int num_args, ...);
		char *(*create_time_rev_phase_pred)(char *op, int num_args, ...);
		char *(*create_time_discuss_phase_pred)(char *op, int num_args, ...);
		char *(*create_time_notify_pred)(char *op, int num_args, ...);
		char *(*create_time_conf_pred)(char *op, int num_args, ...);
	} sql_pred_t;

	extern sql_pred_t hotcrp_mysql_pred;

#ifdef __cplusplus
}
#endif

#endif /* __HOTCRP_SQL_PRED_H__ */
