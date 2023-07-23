#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#include "common/config.h"

#include "qapla.h"

#include "hotcrp/hotcrp_pol_db.h"
#include "hotcrp/hotcrp_db.h"

#define MAX_BUF_LEN	1024

#define ROLE_CHAIR 4


int main(int argc, char *argv[])
{
	if (argc < 2) {
		printf("Usage: ./exe <sql-string>\n");
		printf("E.g.: ./examples/refmon \"select * from A;\"\n");
		return -1;
	}

	const char *sql = argv[1];
	int sqlen = strlen(sql);

	char *dbgfile = "tmp/refmon_hc.dbg";
	char *outfile = "tmp/sql_hc.txt";

	// init query info
	query_info_t qi;
	app_info_t ai = {
		(void *) &hotcrp_mysql_pred,
		(void *) &qapla_hotcrp_cpfn,
		(void *) &qapla_hotcrp_cpcfn,
		&hotcrp_load_schema,
		&hotcrp_load_standard_policies,
	};
	init_query_info(&qi, DB_NAME, dbgfile, &ai);

	// init dlog interpreter env
	const char *email = "foo@xyz.org";
	set_user_session(&qi, "sess0", (char *) email, "pwd");

	int eval_method = PE_METHOD_ROW_SUPP;
	//int eval_method = PE_METHOD_CELL_BLIND;

	int ret = 0;
	char *rewrite_q = NULL;
	int rewrite_qlen = 0;

	// invoke refmon
	ret = run_refmon(&qi, sql, sqlen, 1, eval_method, &rewrite_q,
			&rewrite_qlen);

	// dump rewritten query to a file
	FILE *f = fopen(outfile, "w+");
	if (f) {
		fprintf(f, "use %s;\n", DB_NAME);
		fprintf(f, "%s\n", rewrite_q);
		fclose(f);
	}

	// debug output
	print_query_info(&qi);

	// cleanup
	cleanup_query_info(&qi);

	return EXIT_SUCCESS;
}

