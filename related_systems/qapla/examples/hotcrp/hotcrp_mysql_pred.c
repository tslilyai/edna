#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <ctype.h>

#include "common/config.h"
#include "common/tuple.h"
#include "policyapi/sql_pred.h"
#include "hotcrp_sql_pred.h"

char *
mysql_create_user_role_email_pred(char *op, int num_args, ...)
{
	char pbuf[128];
	memset(pbuf, 0, 128);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char email_buf[32];
	memset(email_buf, 0, 32);
	sprintf(email_buf, "email=");
	int ebuf_len = strlen(email_buf);
	if (type[1] == TTYPE_VARIABLE)
		sprintf(email_buf + ebuf_len, "\':%d\'", *(uint16_t *) val[1]);
	else
		sprintf(email_buf + ebuf_len, "\'%s\'", (char *) val[1]);

	char role_buf[32];
	memset(role_buf, 0, 32);
	if (type[0] == TTYPE_VARIABLE)
		sprintf(role_buf, "roles & :%d != 0", *(uint16_t *) val[0]);
	else {
		if (type[0] == TTYPE_INTEGER && *(uint64_t *) val[0] == ROLE_AUTHOR)
			sprintf(role_buf, "roles=0");
			//sprintf(role_buf, "roles=%d", *(uint64_t *) val[0]);
		else
			sprintf(role_buf, "roles & %d != 0", *(uint64_t *) val[0]);
	}

	sprintf(pbuf, "exists(select 1 from ContactInfo where %s and %s)", 
			email_buf, role_buf);

#if DEBUG
    printf("email buf is %s\n", email_buf);
#endif
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_data_role_pred(char *op, int num_args, ...)
{
	char pbuf[32];
	memset(pbuf, 0, 32);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	if (type[0] == TTYPE_INTEGER) {
		uint64_t var = *(uint64_t *) val[0];
		if (var & ROLE_REVIEWER != 0) {
			//var = var & ~ROLE_REVIEWER;
			sprintf(pbuf, "(ContactInfo.roles & %d != 0)", var);
		} else {
			sprintf(pbuf, "(ContactInfo.roles & %d != 0)", *(uint64_t *) val[0]);
		}
	} else {
		sprintf(pbuf, "(ContactInfo.roles & :%d != 0)", *(uint16_t *) val[0]);
	}

	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_data_contact_email_pred(char *op, int num_args, ...)
{
	char pbuf[32];
	memset(pbuf, 0, 32);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	if (type[0] == TTYPE_VARIABLE)
		sprintf(pbuf, "ContactInfo.email=\':%d\'", *(uint16_t *) val[0]);
	else
		sprintf(pbuf, "ContactInfo.email=\'%s\'", *(char *) val[0]);

	op = add_pred(op, pbuf, strlen(pbuf));
	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_contact_conflict_pred(char *op, int num_args, ...)
{
	char pbuf[1024];
	memset(pbuf, 0, 1024);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char email_buf[32], conflict_buf[64];
	memset(email_buf, 0, 32);
	memset(conflict_buf, 0, 64);
	if (type[0] == TTYPE_VARIABLE)
		sprintf(email_buf, "c.email=\':%d\'", *(uint16_t *) val[0]);
	else
		sprintf(email_buf, "c.email=\'%s\'", (char *) val[0]);

	if (type[1] == TTYPE_INTEGER)
		sprintf(conflict_buf, "conflictType >= %d", *(uint64_t *) val[1]);
	else
		sprintf(conflict_buf, "conflictType >= :%d", *(uint16_t *) val[1]);

	sprintf(pbuf, "contactId in (select contactId from PaperConflict pc1 where "
			"paperId in (select paperId from PaperConflict pc2 join ContactInfo c on "
			"(c.contactId=pc2.contactId) where %s and %s) and %s)",
			email_buf, conflict_buf, conflict_buf);

	op = add_pred(op, pbuf, strlen(pbuf));
	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_accepted_paper_auth_pred(char *op, int num_args, ...)
{
	char pbuf[1024];
	memset(pbuf, 0, 1024);

	sprintf(pbuf, "exists(select 1 from Paper where instr(authorInformation, email) > 0 or instr(authorInformation, concat(firstName, '\t', lastName)) > 0)");
	op = add_pred(op, pbuf, strlen(pbuf));
	return op;
}

char *
mysql_create_contact_paper_sheph_pred(char *op, int num_args, ...)
{
	char pbuf[1024];
	char pbuf1[512], pbuf2[512];
	memset(pbuf, 0, 1024);
	memset(pbuf1, 0, 512);
	memset(pbuf2, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char authbuf[128], rolebuf[64], email_buf[64];
	memset(authbuf, 0, 128);
	memset(rolebuf, 0, 64);
	memset(email_buf, 0, 64);
	if (type[0] == TTYPE_VARIABLE) {
		uint16_t var = *(uint16_t *) val[0];
		sprintf(authbuf, "lower(convert(p.authorInformation using utf8)) like \'%%:%d%%\'", var);
		sprintf(email_buf, "c.email=\':%d\'", var);
	} else {
		char *var = (char *) val[0];
		sprintf(authbuf, "lower(convert(p.authorInformation using utf8)) like \'%%%s%%\'", var);
		sprintf(email_buf, "c.email=\'%s\'", var);
	}

	if (type[1] == TTYPE_VARIABLE)
		sprintf(rolebuf, "(ContactInfo.roles & :%d != 0)", *(uint16_t *) val[1]);
	else
		sprintf(rolebuf, "(ContactInfo.roles & %d != 0)", *(uint64_t *) val[1]);

	sprintf(pbuf1, "exists(select 1 from Paper p where p.shepherdContactId=ContactInfo.contactId and %s and %s)", rolebuf, authbuf);

	sprintf(pbuf2, "exists(select 1 from PaperConflict pc join ContactInfo c on "
			"(c.contactId=pc.contactId and pc.conflictType >= %d and %s) join "
			"Paper p1 on (p1.paperId=pc.paperId) where "
			"p1.shepherdContactId=ContactInfo.contactId and %s)",
			CONFLICT_AUTHOR, email_buf, rolebuf);
	
	sprintf(pbuf, "(%s or %s)", pbuf1, pbuf2);
	op = add_pred(op, pbuf, strlen(pbuf));
	va_end(ap);
	free(type);
	free(val);
	return op;
}

// variable: alias.authorInformation, table.authorInformation
char *
mysql_create_table_paper_author_pred(char *op, int num_args, ...)
{
	char pbuf[1024];
	char pbuf1[512];
	char pbuf2[512];
	memset(pbuf, 0, 1024);
	memset(pbuf1, 0, 512);
	memset(pbuf2, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char *tab_name = (char *) val[0];
	char role_buf[512], user_buf[512], email_buf[64];
	memset(role_buf, 0, 512);
	memset(user_buf, 0, 512);
	memset(email_buf, 0, 64);
	if (type[1] == TTYPE_VARIABLE) {
		uint16_t var = *(uint16_t *) val[1];
		sprintf(user_buf, "lower(convert(p1.authorInformation using utf8)) like \'%%:%d%%\'", var);
		sprintf(email_buf, "c.email=\':%d\'", var);
	} else {
		char *var = (char *) val[1];
		sprintf(user_buf, "lower(convert(p1.authorInformation using utf8)) like \'%%%s%%\'", var);
		sprintf(email_buf, "c.email=\'%s\'", var);
	}

	// for author
	//sprintf(role_buf, "roles = 0)");
	//sprintf(pbuf, "%s%s", user_buf, role_buf);
	
	sprintf(pbuf1, "exists(select 1 from Paper p1 where p1.paperId=%s.paperId and %s)", tab_name, user_buf);

	sprintf(pbuf2, "exists(select 1 from PaperConflict pc join ContactInfo c on "
			"(c.contactId=pc.contactId and pc.conflictType >= %d and %s) join "
			"Paper p1 on (p1.paperId=pc.paperId) where pc.paperId=%s.paperId)",
			CONFLICT_AUTHOR, email_buf, tab_name);
	
	sprintf(pbuf, "(%s or %s)", pbuf1, pbuf2);
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_table_paper_manager(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char cbuf[64];
	memset(cbuf, 0, 64);
	if (type[1] = TTYPE_VARIABLE)
		sprintf(cbuf, "c.email=\':%d\'", *(uint16_t *) val[1]);
	else
		sprintf(cbuf, "c.email=\'%s\'", (char *) val[1]);

	sprintf(pbuf, "exists(select 1 from Paper p join ContactInfo c on "
			"(c.contactId=p.managerContactId) where p.paperId=%s.paperId "
			"and %s)", (char *) val[0], cbuf);

	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_rrating_paper_manager(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char role_buf[512], email_buf[512];
	memset(role_buf, 0, 512);
	memset(email_buf, 0, 512);
	if (type[0] == TTYPE_VARIABLE)
		sprintf(email_buf, "c.email=\':%d\'", *(uint16_t *) val[0]);
	else
		sprintf(email_buf, "c.email=\'%s\'", (char *) val[0]);

	if (type[1] == TTYPE_VARIABLE)
		sprintf(role_buf, " c.roles & :%d != 0", *(uint16_t *) val[1]);
	else
		sprintf(role_buf, " c.roles & %d != 0", *(uint64_t *) val[1]);

	sprintf(pbuf, "exists(select 1 from PaperReview pr join Paper p on "
			"(p.paperId=pr.paperId) join ContactInfo c on (c.contactId=p.managerContactId)"
			" where pr.reviewId=ReviewRating.reviewId and %s and %s)", 
			email_buf, role_buf);
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_paper_submitted_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	if (type[0] == TTYPE_INTEGER)
		sprintf(pbuf, "Paper.timeSubmitted > 0 and Paper.timeWithdrawn <= 0",
				*(uint64_t *) val[0]);
	
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

/* 
 * technically, the predicate should check that timeSubmitted < conf deadline
 * since we currently run our experiments on old dataset, this predicate is
 * meaningless, unless the conf deadline time is in sync with the time configured
 * in the policies.
 */
char *
mysql_create_table_submitted_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char *tab_name = (char *) val[0];
	if (type[1] == TTYPE_INTEGER) {
		sprintf(pbuf, "exists(select 1 from Paper p1 where p1.paperId=%s.paperId and p1.timeSubmitted > 0 and p1.timeWithdrawn <= 0)",
			tab_name, *(uint64_t *) val[1]);
	}

	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_paper_accepted_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	if (type[0] == TTYPE_INTEGER)
		sprintf(pbuf, "Paper.timeSubmitted > 0 and Paper.timeWithdrawn <= 0 and Paper.outcome > 0");
	
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

#if 0
char *
mysql_create_table_accepted_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char *tab_name = (char *) val[0];
	if (type[1] == TTYPE_INTEGER) {
		sprintf(pbuf, "exists(select 1 from Paper p1 where p1.paperId=%s.paperId and p1.timeSubmitted > 0 and p1.timeWithdrawn <= 0 and p1.outcome > 0)", tab_name);
	}

	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}
#endif

char *
mysql_create_table_nonconflict_role_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char *tab_name = (char *) val[0];
	char role_buf[512], user_buf[512], sql_buf[512], sql_buf2[512];
	memset(role_buf, 0, 512);
	memset(user_buf, 0, 512);
	memset(sql_buf, 0, 512);
	memset(sql_buf2, 0, 512);

	if (type[1] == TTYPE_VARIABLE) {
		sprintf(user_buf, "c.email=\':%d\'", *(uint16_t *) val[1]);
	} else {
		sprintf(user_buf, "c.email=\'%s\'", (char *) val[1]);
	}
	if (type[2] == TTYPE_VARIABLE) {
		sprintf(role_buf, "c.roles & :%d !=  0", *(uint16_t *) val[2]);
	} else {
		sprintf(role_buf, "c.roles & %d != 0", *(uint64_t *) val[2]);
	}
	sprintf(sql_buf, "not exists(select 1 from PaperConflict join ContactInfo c "
			"on (c.contactId=PaperConflict.contactId) where "
			"PaperConflict.paperId=%s.paperId and "
			"PaperConflict.conflictType < %d and %s)",
			tab_name, *(uint64_t *) val[3], user_buf);
	sprintf(sql_buf2, "exists(select 1 from ContactInfo c where %s and %s)",
			user_buf, role_buf);

	sprintf(pbuf, "(%s and %s)", sql_buf, sql_buf2);

	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_rrating_nonconflict_role_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char role_buf[512], email_buf[512];
	memset(role_buf, 0, 512);
	memset(email_buf, 0, 512);
	if (type[0] == TTYPE_VARIABLE)
		sprintf(email_buf, "c.email=\':%d\'", *(uint16_t *) val[0]);
	else
		sprintf(email_buf, "c.email=\'%s\'", (char *) val[0]);

	if (type[1] == TTYPE_VARIABLE)
		sprintf(role_buf, " c.roles & :%d != 0", *(uint16_t *) val[1]);
	else
		sprintf(role_buf, " c.roles & %d != 0", *(uint64_t *) val[1]);

	sprintf(pbuf, "not exists(select 1 from PaperReview pr join PaperConflict pc on "
			"(pc.paperId=pr.paperId) join ContactInfo c on (c.contactId=pc.contactId) where "
			"pr.reviewId=ReviewRating.reviewId and %s and %s)", email_buf, role_buf);

	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_pc_role_conflict_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char email_buf[512], role_buf[512];
	memset(email_buf, 0, 512);
	memset(role_buf, 0, 512);
	if (type[0] == TTYPE_VARIABLE)
		sprintf(email_buf, "c.email=\':%d\'", *(uint16_t *) val[0]);
	else
		sprintf(email_buf, "c.email=\'%s\'", (char *) val[0]);

	if (type[1] == TTYPE_INTEGER)
		sprintf(role_buf, "c.roles & %d != 0", *(uint64_t *) val[1]);
	else
		sprintf(role_buf, "c.roles & :%d != 0", *(uint16_t *) val[1]);

	sprintf(pbuf, "exists(select 1 from ContactInfo c where "
			"c.contactId=PaperConflict.contactId and %s and %s)", email_buf, role_buf);
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_pc_conflict_view_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	sprintf(pbuf, "conflictType <= %d", *(uint64_t *) val[0]);
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_author_conflict_pred(char *op, int num_args, ...)
{
	char pbuf[1024];
	char pbuf1[512], pbuf2[512];
	memset(pbuf, 0, 1024);
	memset(pbuf1, 0, 512);
	memset(pbuf2, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char email_buf[64];
	memset(email_buf, 0, 64);
	if (type[0] == TTYPE_VARIABLE) {
		uint16_t var = *(uint16_t *) val[0];
		sprintf(pbuf1, "exists(select 1 from Paper where Paper.paperId=PaperConflict.paperId and lower(convert(authorInformation using utf8)) like \'%%:%d%%\')", var);
		sprintf(email_buf, "c.email=\':%d\'", var);
	} else {
		char *var = (char *) val[0];
		sprintf(pbuf1, "exists(select 1 from Paper where Paper.paperId=PaperConflict.paperId and lower(convert(authorInformation using utf8)) like \'%%%s%%\')", var);
		sprintf(email_buf, "c.email=\'%s\'", var);
	}
	sprintf(pbuf2, "exists(select 1 from PaperConflict pc join ContactInfo c on "
			"(c.contactId=pc.contactId and pc.conflictType >= %d and %s) join "
			"Paper p1 on (p1.paperId=pc.paperId) where "
			"pc.paperId=PaperConflict.paperId)",
			CONFLICT_AUTHOR, email_buf);
	
	sprintf(pbuf, "(%s or %s)", pbuf1, pbuf2);
#if 0
	// authors' marks of PC conflict are only marked by MAXAUTHORMARK and above?
	// values before that are those set by the PC??
	sprintf(conflict_buf, "conflictType >= %d", *(uint64_t *) val[2]);
	sprintf(pbuf, "%s and %s", user_buf, conflict_buf);
#endif
	//sprintf(pbuf, "%s", user_buf);
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_table_rreq_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char *tab = (char *) val[0];
	char role_buf[512], user_buf[512];
	memset(role_buf, 0, 512);
	memset(user_buf, 0, 512);

	if (type[1] == TTYPE_VARIABLE)
		sprintf(user_buf, "c.email=\':%d\'", *(uint16_t *) val[1]);
	else
		sprintf(user_buf, "c.email=\'%s\'", (char *) val[1]);

	if (type[2] == TTYPE_INTEGER)
		sprintf(role_buf, "c.roles & %d != 0", *(uint64_t *) val[2]);
	else
		sprintf(role_buf, "c.roles & :%d != 0", *(uint16_t *) val[2]);

	sprintf(pbuf, "exists(select 1 from ContactInfo c where c.contactId=%s.requestedBy"
			" and %s and %s)", tab, user_buf, role_buf);
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_table_self_revpref_pred(char *op, int num_args, ...)
{
	char pbuf[1024];
	memset(pbuf, 0, 1024);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char email_buf[64], role_buf[64];
	memset(email_buf, 0, 64);
	memset(role_buf, 0, 64);
	if (type[0] == TTYPE_VARIABLE)
		sprintf(email_buf, "c.email=\':%d\'", *(uint16_t *) val[0]);
	else
		sprintf(email_buf, "c.email=\'%s\'", (char *) val[0]);

	if (type[1] == TTYPE_VARIABLE)
		sprintf(role_buf, " c.roles & :%d != 0", *(uint16_t *) val[1]);
	else
		sprintf(role_buf, " c.roles & %d != 0", *(uint64_t *) val[1]);

	sprintf(pbuf, "exists(select 1 from PaperReviewPreference prp join "
			"PaperReview pr on (pr.paperId=prp.paperId) join ContactInfo c on "
			"(c.contactId=prp.contactId) where prp.paperId=PaperReviewPreference.paperId"
			" and %s and %s)", email_buf, role_buf);
	//sprintf(pbuf, "exists(select 1 from PaperReview pr join ContactInfo c on "
	//		"(c.contactId=pr.contactId) where "
	//		"pr.paperId=PaperReviewPreference.paperId "
	//		"and %s and %s)", email_buf, role_buf);

	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_is_review_submitted_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	sprintf(pbuf, "reviewSubmitted > 0 and reviewNeedsSubmit <= 0");
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_review_requester_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char user_buf[512];
	memset(user_buf, 0, 512);
	if (type[0] == TTYPE_VARIABLE) {
		sprintf(user_buf, "email=\':%d\'", *(uint16_t *) val[0]);
	} else {
		sprintf(user_buf, "email=\'%s\'", (char *) val[0]);
	}

	sprintf(pbuf, "exists(select 1 from ContactInfo where "
			"contactId=PaperReview.requestedBy and %s)", user_buf);
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_table_submitted_review_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char *tab_name = (char *) val[0];
	char role_buf[512], email_buf[512];
	memset(role_buf, 0, 512);
	memset(email_buf, 0, 512);
	if (type[1] == TTYPE_VARIABLE)
		sprintf(email_buf, "c.email=\':%d\'", *(uint16_t *) val[1]);
	else
		sprintf(email_buf, "c.email=\'%s\'", (char *) val[1]);

	if (type[2] == TTYPE_VARIABLE)
		sprintf(role_buf, " c.roles & :%d != 0", *(uint16_t *) val[2]);
	else
		sprintf(role_buf, " c.roles & %d != 0", *(uint64_t *) val[2]);

	sprintf(pbuf, "exists(select 1 from PaperReview r join ContactInfo c on "
			"(c.contactId=r.contactId) where r.paperId=%s.paperId and "
			"r.reviewSubmitted > 0 and %s and %s)", tab_name, email_buf, role_buf);
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_rrating_submitted_review_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char role_buf[512], email_buf[512];
	memset(role_buf, 0, 512);
	memset(email_buf, 0, 512);
	if (type[0] == TTYPE_VARIABLE)
		sprintf(email_buf, "c.email=\':%d\'", *(uint16_t *) val[0]);
	else
		sprintf(email_buf, "c.email=\'%s\'", (char *) val[0]);

	if (type[1] == TTYPE_VARIABLE)
		sprintf(role_buf, " c.roles & :%d != 0", *(uint16_t *) val[1]);
	else
		sprintf(role_buf, " c.roles & %d != 0", *(uint64_t *) val[1]);

	sprintf(pbuf, "exists(select 1 from PaperReview r join ContactInfo c on "
			"(c.contactId=r.contactId) where r.reviewId=ReviewRating.reviewId and "
			"r.reviewSubmitted > 0 and %s and %s)", email_buf, role_buf);
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_comment_compare_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	uint64_t op_type = *(uint64_t *) val[0];
	uint64_t ctype = *(uint64_t *) val[1];
	char *op_sign = get_op_name((int) op_type);
	if (op_type == OP_NE)
		sprintf(pbuf, "commentType & %d = 0", ctype);
	else
		sprintf(pbuf, "commentType & %d != 0", ctype);

	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_comment_ordinal_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	sprintf(pbuf, "authorOrdinal > 0 and ordinal=0");
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

char *
mysql_create_self_topic_interest_pred(char *op, int num_args, ...)
{
	char pbuf[512];
	memset(pbuf, 0, 512);
	va_list ap;

	va_start(ap, num_args);
	uint32_t *type = (uint32_t *) malloc(sizeof(uint32_t) * num_args);
	uint8_t **val = (uint8_t **) malloc(sizeof(uint8_t *) * num_args);

	int i;
	for (i = 0; i < num_args; i++) {
		type[i] = va_arg(ap, uint32_t);
		val[i] = va_arg(ap, uint8_t *);
		if (type[i] == TTYPE_VARLEN)
			(void) va_arg(ap, uint8_t *);
	}

	char email_buf[512];
	memset(email_buf, 0, 512);
	if (type[0] == TTYPE_VARIABLE) {
		sprintf(email_buf, "c.email=\':%d\'", *(uint16_t *) val[0]);
	} else {
		sprintf(email_buf, "c.email=\'%s\'", (char *) val[0]);
	}
	sprintf(pbuf, "exists(select 1 from ContactInfo c where "
			"c.contactId=TopicInterest.contactId and %s)", email_buf);
	op = add_pred(op, pbuf, strlen(pbuf));

	va_end(ap);
	free(type);
	free(val);
	return op;
}

sql_pred_t hotcrp_mysql_pred = {
	mysql_create_user_role_email_pred,
	mysql_create_data_role_pred,
	mysql_create_data_contact_email_pred,
	mysql_create_contact_conflict_pred,
	mysql_create_accepted_paper_auth_pred,
	mysql_create_contact_paper_sheph_pred,
	mysql_create_table_paper_author_pred,
	mysql_create_table_paper_manager,
	mysql_create_rrating_paper_manager,
	mysql_create_paper_submitted_pred,
	mysql_create_table_submitted_pred,
	mysql_create_paper_accepted_pred,
	mysql_create_table_nonconflict_role_pred,
	mysql_create_rrating_nonconflict_role_pred,
	mysql_create_table_rreq_pred,
	mysql_create_table_self_revpref_pred,
	mysql_create_is_review_submitted_pred,
	mysql_create_review_requester_pred,
	mysql_create_table_submitted_review_pred,
	mysql_create_rrating_submitted_review_pred,
	mysql_create_comment_compare_pred,
	mysql_create_comment_ordinal_pred,
	mysql_create_pc_role_conflict_pred,
	mysql_create_pc_conflict_view_pred,
	mysql_create_author_conflict_pred,
	mysql_create_self_topic_interest_pred,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL,
	NULL
};
