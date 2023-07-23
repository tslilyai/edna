#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "hotcrp_db.h"
#include "hotcrp_pol_db.h"
#include "hotcrp_sql_pred.h"
#include "common/qapla_policy.h"

extern sql_pred_t *g_sql_pred;

int
hotcrp_load_schema(db_t *db)
{
	if (!db)
		return -1;

	int ret = 0, tid = 0, cid = 0;
	char t_buf[MAX_NAME_LEN];
	memset(t_buf, 0, MAX_NAME_LEN);

	int i;
	sprintf(t_buf, "%s", "ActionLog");
	ret |= add_col_in_table(db, (const char *) t_buf, "logId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "contactId", &cid);
	ret |= add_col_in_table_id(db, tid, "paperId", &cid);
	ret |= add_col_in_table_id(db, tid, "time", &cid);
	ret |= add_col_in_table_id(db, tid, "ipaddr", &cid);
	ret |= add_col_in_table_id(db, tid, "action", &cid);

	sprintf(t_buf, "%s", "Capability");
	ret |= add_col_in_table(db, (const char *) t_buf, "capabilityId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "capabilityType", &cid);
	ret |= add_col_in_table_id(db, tid, "contactId", &cid);
	ret |= add_col_in_table_id(db, tid, "paperId", &cid);
	ret |= add_col_in_table_id(db, tid, "timeExpires", &cid);
	ret |= add_col_in_table_id(db, tid, "salt", &cid);
	ret |= add_col_in_table_id(db, tid, "data", &cid);

	sprintf(t_buf, "%s", "ContactInfo");
	ret |= add_col_in_table(db, (const char *) t_buf, "contactId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "firstName", &cid);
	ret |= add_col_in_table_id(db, tid, "firstName", &cid);
	ret |= add_col_in_table_id(db, tid, "lastName", &cid);
	ret |= add_col_in_table_id(db, tid, "email", &cid);
	ret |= add_col_in_table_id(db, tid, "preferredEmail", &cid);
	ret |= add_col_in_table_id(db, tid, "affiliation", &cid);
	ret |= add_col_in_table_id(db, tid, "voicePhoneNumber", &cid);
	ret |= add_col_in_table_id(db, tid, "faxPhoneNumber", &cid);
	ret |= add_col_in_table_id(db, tid, "password", &cid);
	ret |= add_col_in_table_id(db, tid, "collaborators", &cid);
	ret |= add_col_in_table_id(db, tid, "creationTime", &cid);
	ret |= add_col_in_table_id(db, tid, "lastLogin", &cid);
	ret |= add_col_in_table_id(db, tid, "defaultWatch", &cid);
	ret |= add_col_in_table_id(db, tid, "roles", &cid);
	ret |= add_col_in_table_id(db, tid, "contactTags", &cid);
	ret |= add_col_in_table_id(db, tid, "disabled", &cid);
	ret |= add_col_in_table_id(db, tid, "data", &cid);
	ret |= add_col_in_table_id(db, tid, "passwordTime", &cid);
	ret |= add_col_in_table_id(db, tid, "unaccentedName", &cid);
	ret |= add_col_in_table_id(db, tid, "passwordIsCdb", &cid);

	sprintf(t_buf, "%s", "Formula");
	ret |= add_col_in_table(db, (const char *) t_buf, "formulaId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "name", &cid);
	ret |= add_col_in_table_id(db, tid, "heading", &cid);
	ret |= add_col_in_table_id(db, tid, "headingTitle", &cid);
	ret |= add_col_in_table_id(db, tid, "expression", &cid);
	ret |= add_col_in_table_id(db, tid, "authorView", &cid);
	ret |= add_col_in_table_id(db, tid, "createdBy", &cid);
	ret |= add_col_in_table_id(db, tid, "timeModified", &cid);

	sprintf(t_buf, "%s", "MailLog");
	ret |= add_col_in_table(db, (const char *) t_buf, "mailId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "recipients", &cid);
	ret |= add_col_in_table_id(db, tid, "paperIds", &cid);
	ret |= add_col_in_table_id(db, tid, "cc", &cid);
	ret |= add_col_in_table_id(db, tid, "replyTo", &cid);
	ret |= add_col_in_table_id(db, tid, "subject", &cid);
	ret |= add_col_in_table_id(db, tid, "emailBody", &cid);
	ret |= add_col_in_table_id(db, tid, "q", &cid);
	ret |= add_col_in_table_id(db, tid, "t", &cid);

	sprintf(t_buf, "%s", "Paper");
	ret |= add_col_in_table(db, (const char *) t_buf, "paperId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "title", &cid);
	ret |= add_col_in_table_id(db, tid, "authorInformation", &cid);
	ret |= add_col_in_table_id(db, tid, "abstract", &cid);
	ret |= add_col_in_table_id(db, tid, "collaborators", &cid);
	ret |= add_col_in_table_id(db, tid, "timeSubmitted", &cid);
	ret |= add_col_in_table_id(db, tid, "timeWithdrawn", &cid);
	ret |= add_col_in_table_id(db, tid, "timeFinalSubmitted", &cid);
	ret |= add_col_in_table_id(db, tid, "paperStorageId", &cid);
	ret |= add_col_in_table_id(db, tid, "sha1", &cid);
	ret |= add_col_in_table_id(db, tid, "finalPaperStorageId", &cid);
	ret |= add_col_in_table_id(db, tid, "blind", &cid);
	ret |= add_col_in_table_id(db, tid, "outcome", &cid);
	ret |= add_col_in_table_id(db, tid, "leadContactId", &cid);
	ret |= add_col_in_table_id(db, tid, "shepherdContactId", &cid);
	ret |= add_col_in_table_id(db, tid, "size", &cid);
	ret |= add_col_in_table_id(db, tid, "mimetype", &cid);
	ret |= add_col_in_table_id(db, tid, "timestamp", &cid);
	ret |= add_col_in_table_id(db, tid, "capVersion", &cid);
	ret |= add_col_in_table_id(db, tid, "withdrawReason", &cid);
	ret |= add_col_in_table_id(db, tid, "managerContactId", &cid);

	sprintf(t_buf, "%s", "PaperComment");
	ret |= add_col_in_table(db, (const char *) t_buf, "commentId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "contactId", &cid);
	ret |= add_col_in_table_id(db, tid, "paperId", &cid);
	ret |= add_col_in_table_id(db, tid, "timeModified", &cid);
	ret |= add_col_in_table_id(db, tid, "timeNotified", &cid);
	ret |= add_col_in_table_id(db, tid, "comment", &cid);
	ret |= add_col_in_table_id(db, tid, "replyTo", &cid);
	ret |= add_col_in_table_id(db, tid, "paperStorageId", &cid);
	ret |= add_col_in_table_id(db, tid, "ordinal", &cid);
	ret |= add_col_in_table_id(db, tid, "commentType", &cid);
	ret |= add_col_in_table_id(db, tid, "commentTags", &cid);
	ret |= add_col_in_table_id(db, tid, "commentRound", &cid);
	ret |= add_col_in_table_id(db, tid, "commentFormat", &cid);
	ret |= add_col_in_table_id(db, tid, "authorOrdinal", &cid);

	sprintf(t_buf, "%s", "PaperConflict");
	ret |= add_col_in_table(db, (const char *) t_buf, "paperId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "contactId", &cid);
	ret |= add_col_in_table_id(db, tid, "conflictType", &cid);

	sprintf(t_buf, "%s", "PaperOption");
	ret |= add_col_in_table(db, (const char *) t_buf, "paperId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "optionId", &cid);
	ret |= add_col_in_table_id(db, tid, "value", &cid);
	ret |= add_col_in_table_id(db, tid, "data", &cid);

	sprintf(t_buf, "%s", "PaperReview");
	ret |= add_col_in_table(db, (const char *) t_buf, "reviewId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "paperId", &cid);
	ret |= add_col_in_table_id(db, tid, "contactId", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewToken", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewType", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewRound", &cid);
	ret |= add_col_in_table_id(db, tid, "requestedBy", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewBlind", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewModified", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewSubmitted", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewNotified", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewOrdinal", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewEditVersion", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewNeedsSubmit", &cid);
	ret |= add_col_in_table_id(db, tid, "overAllMerit", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewerQualification", &cid);
	ret |= add_col_in_table_id(db, tid, "novelty", &cid);
	ret |= add_col_in_table_id(db, tid, "technicalMerit", &cid);
	ret |= add_col_in_table_id(db, tid, "interestToCommunity", &cid);
	ret |= add_col_in_table_id(db, tid, "longevity", &cid);
	ret |= add_col_in_table_id(db, tid, "grammar", &cid);
	ret |= add_col_in_table_id(db, tid, "likelyPresentation", &cid);
	ret |= add_col_in_table_id(db, tid, "suitableForShort", &cid);
	ret |= add_col_in_table_id(db, tid, "paperSummary", &cid);
	ret |= add_col_in_table_id(db, tid, "commentsToAuthor", &cid);
	ret |= add_col_in_table_id(db, tid, "commentsToPC", &cid);
	ret |= add_col_in_table_id(db, tid, "commentsToAddress", &cid);
	ret |= add_col_in_table_id(db, tid, "weaknessOfPaper", &cid);
	ret |= add_col_in_table_id(db, tid, "strengthOfPaper", &cid);
	ret |= add_col_in_table_id(db, tid, "potential", &cid);
	ret |= add_col_in_table_id(db, tid, "fixability", &cid);
	ret |= add_col_in_table_id(db, tid, "textField7", &cid);
	ret |= add_col_in_table_id(db, tid, "textField8", &cid);
	ret |= add_col_in_table_id(db, tid, "timeRequested", &cid);
	ret |= add_col_in_table_id(db, tid, "timeRequestNotified", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewAuthorNotified", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewAuthorSeen", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewWordCount", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewFormat", &cid);

	sprintf(t_buf, "%s", "PaperReviewArchive");
	ret |= add_col_in_table(db, (const char *) t_buf, "reviewArchiveId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "reviewId", &cid);
	ret |= add_col_in_table_id(db, tid, "paperId", &cid);
	ret |= add_col_in_table_id(db, tid, "contactId", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewToken", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewType", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewRound", &cid);
	ret |= add_col_in_table_id(db, tid, "requestedBy", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewBlind", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewModified", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewSubmitted", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewNotified", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewOrdinal", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewEditVersion", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewNeedsSubmit", &cid);
	ret |= add_col_in_table_id(db, tid, "overAllMerit", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewerQualification", &cid);
	ret |= add_col_in_table_id(db, tid, "novelty", &cid);
	ret |= add_col_in_table_id(db, tid, "technicalMerit", &cid);
	ret |= add_col_in_table_id(db, tid, "interestToCommunity", &cid);
	ret |= add_col_in_table_id(db, tid, "longevity", &cid);
	ret |= add_col_in_table_id(db, tid, "grammar", &cid);
	ret |= add_col_in_table_id(db, tid, "likelyPresentation", &cid);
	ret |= add_col_in_table_id(db, tid, "suitableForShort", &cid);
	ret |= add_col_in_table_id(db, tid, "paperSummary", &cid);
	ret |= add_col_in_table_id(db, tid, "commentsToAuthor", &cid);
	ret |= add_col_in_table_id(db, tid, "commentsToPC", &cid);
	ret |= add_col_in_table_id(db, tid, "commentsToAddress", &cid);
	ret |= add_col_in_table_id(db, tid, "weaknessOfPaper", &cid);
	ret |= add_col_in_table_id(db, tid, "strengthOfPaper", &cid);
	ret |= add_col_in_table_id(db, tid, "potential", &cid);
	ret |= add_col_in_table_id(db, tid, "fixability", &cid);
	ret |= add_col_in_table_id(db, tid, "textField7", &cid);
	ret |= add_col_in_table_id(db, tid, "textField8", &cid);
	ret |= add_col_in_table_id(db, tid, "timeRequested", &cid);
	ret |= add_col_in_table_id(db, tid, "timeRequestNotified", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewAuthorNotified", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewAuthorSeen", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewWordCount", &cid);
	ret |= add_col_in_table_id(db, tid, "reviewFormat", &cid);

	sprintf(t_buf, "%s", "PaperReviewPreference");
	ret |= add_col_in_table(db, (const char *) t_buf, "paperId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "contactId", &cid);
	ret |= add_col_in_table_id(db, tid, "preference", &cid);
	ret |= add_col_in_table_id(db, tid, "expertise", &cid);

	sprintf(t_buf, "%s", "PaperReviewRefused");
	ret |= add_col_in_table(db, (const char *) t_buf, "paperId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "contactId", &cid);
	ret |= add_col_in_table_id(db, tid, "requestedBy", &cid);
	ret |= add_col_in_table_id(db, tid, "reason", &cid);

	sprintf(t_buf, "%s", "PaperStorage");
	ret |= add_col_in_table(db, (const char *) t_buf, "paperStorageId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "paperId", &cid);
	ret |= add_col_in_table_id(db, tid, "timestamp", &cid);
	ret |= add_col_in_table_id(db, tid, "mimetype", &cid);
	ret |= add_col_in_table_id(db, tid, "paper", &cid);
	ret |= add_col_in_table_id(db, tid, "compression", &cid);
	ret |= add_col_in_table_id(db, tid, "sha1", &cid);
	ret |= add_col_in_table_id(db, tid, "documentType", &cid);
	ret |= add_col_in_table_id(db, tid, "filename", &cid);
	ret |= add_col_in_table_id(db, tid, "infoJson", &cid);
	ret |= add_col_in_table_id(db, tid, "size", &cid);
	ret |= add_col_in_table_id(db, tid, "filterType", &cid);
	ret |= add_col_in_table_id(db, tid, "originalStorageId", &cid);

	sprintf(t_buf, "%s", "PaperTag");
	ret |= add_col_in_table(db, (const char *) t_buf, "paperId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "tag", &cid);
	ret |= add_col_in_table_id(db, tid, "tagIndex", &cid);

	sprintf(t_buf, "%s", "PaperTopic");
	ret |= add_col_in_table(db, (const char *) t_buf, "topicId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "paperId", &cid);

	sprintf(t_buf, "%s", "PaperWatch");
	ret |= add_col_in_table(db, (const char *) t_buf, "paperId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "contactId", &cid);
	ret |= add_col_in_table_id(db, tid, "watch", &cid);

	sprintf(t_buf, "%s", "ReviewRating");
	ret |= add_col_in_table(db, (const char *) t_buf, "reviewId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "contactId", &cid);
	ret |= add_col_in_table_id(db, tid, "rating", &cid);

	sprintf(t_buf, "%s", "ReviewRequest");
	ret |= add_col_in_table(db, (const char *) t_buf, "paperId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "name", &cid);
	ret |= add_col_in_table_id(db, tid, "email", &cid);
	ret |= add_col_in_table_id(db, tid, "reason", &cid);
	ret |= add_col_in_table_id(db, tid, "requestedBy", &cid);

	sprintf(t_buf, "%s", "Settings");
	ret |= add_col_in_table(db, (const char *) t_buf, "name", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "value", &cid);
	ret |= add_col_in_table_id(db, tid, "data", &cid);

	sprintf(t_buf, "%s", "TopicArea");
	ret |= add_col_in_table(db, (const char *) t_buf, "topicId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "topicName", &cid);

	sprintf(t_buf, "%s", "TopicInterest");
	ret |= add_col_in_table(db, (const char *) t_buf, "contactId", &tid, &cid);
	ret |= add_col_in_table_id(db, tid, "topicId", &cid);
	ret |= add_col_in_table_id(db, tid, "interest", &cid);

	return ret;
}

void
hotcrp_load_standard_policies(db_t *schema, metadata_t *qm,
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

	// == contact info ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_contact_info_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_contact_info_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == contact email ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_contact_email_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_contact_email_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == contact id ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_contact_id_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_contact_id_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == contact PC view only ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_contact_pc_view_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_contact_pc_view_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == contact private ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_contact_private_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_contact_private_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == contact owner and chair view ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_contact_chair_owner_view_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_contact_chair_owner_view_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == paper outcome ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_outcome_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_outcome_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == paper shepherd == 
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_shepherd_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_shepherd_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == paper lead ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_lead_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_lead_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == paper manager ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_manager_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_manager_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == author's collaborators ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	//g_cpfn->create_paper_collab_policy(schema, g_sql_pred, qp, &pol_len);
	g_cpfn->create_paper_author_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_collab_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == paper metadata == 
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	//g_cpfn->_create_sql_true_policy(schema, (char *) "Paper", qp, &pol_len);
	g_cpfn->create_paper_meta_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_meta_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == pstore ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_pstore_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_pstore_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == review public ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_review_for_author_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_review_for_author_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == review private ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_review_private_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_review_private_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == review rating ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_review_rating_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_review_rating_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == conflicts ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_conflict_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);
	
	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_conflict_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == paper tags ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_papertags_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_papertags_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == paper options ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_options_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_options_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == comments public ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_comment_for_author_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_comment_for_author_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == comments private ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_private_comment_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_private_comment_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == topic interest ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_topic_interest_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_topic_interest_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == paper topic ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_topic_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_topic_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == action log ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_action_log_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_action_log_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == mail log ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_mail_log_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_mail_log_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == review request ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_revrequest_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_revrequest_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == review refused ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_revrefused_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_revrefused_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == review preference ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_revpreference_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_revpreference_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == paper watch policy ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paperwatch_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paperwatch_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	
	// == settings ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->_create_sql_true_policy(schema, g_sql_pred,
			(char *) "Settings", qp, &pol_len);
	sprintf(qpp->alias, "settings_true");
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_settings_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == topicarea ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->_create_sql_true_policy(schema, g_sql_pred,
			(char *) "TopicArea", qp, &pol_len);
	sprintf(qpp->alias, "topicarea_true");
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_topicarea_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == formula ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->_create_sql_true_policy(schema, g_sql_pred,
			(char *) "Formula", qp, &pol_len);
	sprintf(qpp->alias, "formula_true");
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_formula_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == review true policy ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->_create_sql_true_policy(schema, g_sql_pred,
			(char *) "PaperReview", qp, &pol_len);
	sprintf(qpp->alias, "review_true");
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->_add_review_true_col(schema, &cs_list);
	add_policy_to_pvec_index(c_pv, pid, &cs_list);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	metadata_add_policy_cluster(qm, c);

	// cluster 2
	c = (cluster_t *) malloc(sizeof(cluster_t));
	cluster_id = metadata_get_next_policy_cluster_id(qm);
	init_cluster(c, cluster_id, OP_AND, C_NON_AGGR);

	// == title,abstract ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_title_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);
	uint16_t title_pid = pid;

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_title_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == topic(title) link ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_title_topic_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);
	uint16_t title_topic_pid = pid;

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_title_topic_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	cluster_set_pid_for_col_list(c, &cs_list, title_pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	//cluster_set_pid_for_col_list(c, &cs_list, title_auth_pid);

	cluster_set_less_restrictive_pid_list(c, title_pid, &title_topic_pid, 1);
	//cluster_set_less_restrictive_pid_list(c, title_auth_pid, &title_topic_pid, 1);

	metadata_add_policy_cluster(qm, c);

	// cluster 3
	c = (cluster_t *) malloc(sizeof(cluster_t));
	cluster_id = metadata_get_next_policy_cluster_id(qm);
	init_cluster(c, cluster_id, OP_AND, C_NON_AGGR);

	// == author ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_author_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);
	uint16_t auth_pid = pid;

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_author_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == affil(author) link ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_author_affil_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);
	uint16_t author_affil_pid = pid;

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_author_affil_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	cluster_set_pid_for_col_list(c, &cs_list, auth_pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	cluster_set_less_restrictive_pid_list(c, auth_pid, &author_affil_pid, 1);

	metadata_add_policy_cluster(qm, c);

	// put all aggregate policies in this cluster, 
	// they are also unlinkable, so the cluster op is AND
	// cluster 4
	c = (cluster_t *) malloc(sizeof(cluster_t));
	cluster_id = metadata_get_next_policy_cluster_id(qm);
	init_cluster(c, cluster_id, OP_AND, C_AGGR);
	
	// == avg overall merit ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_avg_overall_merit_pc_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);
	uint16_t avg_score_pid = pid;

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_avg_overall_merit_pc_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == aggr review status by pid ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_aggr_review_status_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_aggr_rev_status_by_pid_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == aggr review status by cid ==
	// policy is same as above, but create diff. instance
	pid = get_next_pol_id(qpool);
	qapla_set_policy_id(qpp, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_aggr_rev_status_by_cid_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);

	// == combined stats on review status and avg score ==
	// policy same as above, but create diff. instance
	// this policy is explicitly required to prevent the link of all cols 
	// from being subject to false policy
	pid = get_next_pol_id(qpool);
	qapla_set_policy_id(qpp, pid);
	uint16_t combined_stats_pid = pid;

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_aggr_rev_status_by_cid_col(schema, &cs_list);
	g_cpcfn->create_avg_overall_merit_pc_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	cluster_set_less_restrictive_pid_list(c, combined_stats_pid, &avg_score_pid, 1);

	// == aggr submissions group by outcome ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_aggr_group_outcome_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_aggr_group_outcome_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	uint16_t aggr_group_by_outcome_pid = pid;

	// == aggr submissions group by topic ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_aggr_topic_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_aggr_topic_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	uint16_t paper_aggr_topic_pid = pid;

	cluster_set_less_restrictive_pid_list(c, paper_aggr_topic_pid, 
			&aggr_group_by_outcome_pid, 1);

	// == aggr submissions group by affiliation ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_aggr_affil_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_aggr_affil_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	uint16_t paper_aggr_affil_pid = pid;

	cluster_set_less_restrictive_pid_list(c, paper_aggr_affil_pid, 
			&aggr_group_by_outcome_pid, 1);

	// == aggr submissions group by topic,country ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->create_paper_aggr_topic_country_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_paper_aggr_topic_country_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	uint16_t paper_aggr_topic_country_pid = pid;

	cluster_set_less_restrictive_pid_list(c, paper_aggr_topic_country_pid, 
			&aggr_group_by_outcome_pid, 1);
	cluster_set_less_restrictive_pid_list(c, paper_aggr_topic_country_pid,
			&paper_aggr_topic_pid, 1);

	// == aggr review group by topic ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	g_cpfn->_create_review_paper_analytics_policy(schema, g_sql_pred, qp, &pol_len);
	sprintf(qpp->alias, "review_aggr_by_topic");
	//g_cpfn->create_review_paper_aggr_topic_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_review_paper_aggr_topic_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	uint16_t aggr_rev_group_by_topic_pid = pid;

	cluster_set_less_restrictive_pid_list(c, aggr_rev_group_by_topic_pid,
			&aggr_group_by_outcome_pid, 1);
	cluster_set_less_restrictive_pid_list(c, aggr_rev_group_by_topic_pid,
			&paper_aggr_topic_pid, 1);

	// == aggr review group by affil ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	memset(qpp->alias, 0, MAX_ALIAS_LEN);
	sprintf(qpp->alias, "review_aggr_by_affil");
	// same policy as above, different instance
	//g_cpfn->create_review_paper_aggr_affil_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_review_paper_aggr_affil_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);
	uint16_t aggr_rev_group_by_affil_pid = pid;

	cluster_set_less_restrictive_pid_list(c, aggr_rev_group_by_affil_pid,
			&aggr_group_by_outcome_pid, 1);
	cluster_set_less_restrictive_pid_list(c, aggr_rev_group_by_affil_pid,
			&paper_aggr_affil_pid, 1);

	// == avg review score group by topic ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	memset(qpp->alias, 0, MAX_ALIAS_LEN);
	sprintf(qpp->alias, "avg_score_by_topic");
	//g_cpfn->create_avg_overall_merit_topic_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_avg_overall_merit_topic_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	// == avg review score group by affil ==
	memset(&qp_buf, 0, sizeof(qapla_policy_t));
	pid = get_next_pol_id(qpool);
	memset(qpp->alias, 0, MAX_ALIAS_LEN);
	sprintf(qpp->alias, "avg_score_by_affil");
	//g_cpfn->create_avg_overall_merit_affil_policy(schema, g_sql_pred, qp, &pol_len);
	qapla_set_policy_id(qpp, pid);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);
	g_cpcfn->create_avg_overall_merit_affil_col(schema, &cs_list);
	cluster_set_pid_for_col_list(c, &cs_list, pid);
	add_policy_to_pool(qpool, qp, pol_len, &cs_list);

	metadata_add_policy_cluster(qm, c);

	cleanup_col_sym_list(&cs_list);
	list_init(&cs_list);

#if DEBUG
	printf("TOTAL POLICIES: %d\n", get_num_policy_in_pool(qpool));
#endif
}
