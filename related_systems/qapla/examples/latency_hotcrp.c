#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "common/config.h"
#include "utils/stats_defs.h"
#include "utils/statistics.h"
#include "hotcrp/hotcrp_pol_db.h"
#include "hotcrp/hotcrp_db.h"
#include "qapla.h"

#define PRINT_OUTPUT 1

STATS_DEFS_MAP(DECL_STAT_EXTERN);

enum {
	ARG_CONFIG = 1,
	ARG_EXP_ITER,
	ARG_PE_METHOD,
	ARG_EMAIL,
	ARG_SQL,
	MAX_ARGS
};

int main(int argc, char *argv[])
{
	if (argc < MAX_ARGS) {
		printf("Usage: ./exe <config> <#iter> <enf-method-type> <userid> <query>\n");
		printf("E.g.: ./examples/latency <[0|1]> 3 <[0|1]> \"xxx@yyy.com\" "
				"\"select * from A;\"\n");
		printf("config: 0 - baseline, 1 - qapla\n");
		printf("enf-method-type: 0 - Row suppression, 1 - Cell blinding\n");
		return -1;
	}

	int config = atoi(argv[ARG_CONFIG]);
	int exp_iter = atoi(argv[ARG_EXP_ITER]);
	int pe_method = atoi(argv[ARG_PE_METHOD]);
	const char *email = argv[ARG_EMAIL];
	const char *sql = argv[ARG_SQL];
	int sqlen = strlen(sql);
	int res;
	int ret;
	int i, j;

	query_info_t qi;
	char *rewritten_q = (char *) sql;
	int rewritten_qlen = sqlen;
	char *translate_q = NULL;
	int translate_qlen = 0;
	char *res_buf = NULL;
	int nrow, ncol;

	app_info_t ai = {
		(void *) &hotcrp_mysql_pred,
		(void *) &qapla_hotcrp_cpfn,
		(void *) &qapla_hotcrp_cpcfn,
		&hotcrp_load_schema,
		&hotcrp_load_standard_policies,
	};
	_init_query_info(&qi, DB_NAME, "/tmp/client.txt", DB_MYSQL, &ai);
	set_user_session(&qi, "sess0", (char *) email, "pwd");

	STAT_START_TIMER(RUNTIME);

	for (i = 0; i < exp_iter; i++) {
		STAT_START_TIMER(ITER_RUNTIME);
		if (config) {
			ret = run_refmon(&qi, sql, sqlen, 1, pe_method, &rewritten_q, &rewritten_qlen);
		} else {
			STAT_START_TIMER(QRM_REWRITE);
			rewritten_q = (char *) malloc(sqlen+2);
			memset(rewritten_q, 0, sqlen+2);
			memcpy(rewritten_q, sql, sqlen);
			memcpy(rewritten_q+sqlen, ";", 1);
			rewritten_qlen = sqlen+2;
			STAT_END_TIMER(QRM_REWRITE);
		}

#if PRINT_OUTPUT
		FILE *f = fopen("/tmp/sql.txt", "w+");
		if (f) {
#if CONFIG_DB == DB_MYSQL
				fprintf(f, "use %s;\n", DB_NAME);
#endif
			fprintf(f, "%s\n", rewritten_q);
			fclose(f);
		}
#endif

		STAT_END_TIMER(ITER_RUNTIME);
	}

	STAT_END_TIMER(RUNTIME);

	cleanup_query_info(&qi);
}
