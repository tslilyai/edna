#include "refmon_ws_lib.h"

void rewrite_query(char* email, char* sql, char** rewrite_q, bool cellblind)
{
	int sqlen = strlen(sql);
	char *dbgfile = "tmp/refmon_ws.dbg";

	// init query info
	query_info_t qi;
	app_info_t ai = {
		(void *) &websubmit_mysql_pred,
		(void *) &qapla_websubmit_cpfn,
		(void *) &qapla_websubmit_cpcfn,
		&websubmit_load_schema,
		&websubmit_load_standard_policies,
	};
	init_query_info(&qi, DB_NAME, dbgfile, &ai);

	// init dlog interpreter env
	set_user_session(&qi, "sess0", (char *) email, "pwd");

	int eval_method;
	if (!cellblind) {
		eval_method = PE_METHOD_ROW_SUPP;
	} else {
		eval_method = PE_METHOD_CELL_BLIND;
	}

	int ret = 0;
	int rewrite_qlen = 0;

#if DEBUG
	printf("rewrite_query: rewriting %s\n", sql);
#endif
	// invoke refmon
	ret = run_refmon(&qi, sql, sqlen, 1, eval_method, rewrite_q, &rewrite_qlen);
#if DEBUG
	printf("rewrite_query: rewritten to %s len %d, saving at %p\n", *rewrite_q, rewrite_qlen, rewrite_q);
#endif

	// debug output
	//print_query_info(&qi);
}
