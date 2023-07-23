#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

#include "common/config.h"

#include "qapla.h"

#include "hotcrp/hotcrp_pol_db.h"
#include "hotcrp/hotcrp_db.h"

int main(int argc, char *argv[])
{
	char *sql = argv[1];
	int sqlen = strlen(sql);

	list_t param_list;
	list_init(&param_list);

	query_info_t qi;
	app_info_t ai = {
		(void *) &hotcrp_mysql_pred,
		(void *) &qapla_hotcrp_cpfn,
		(void *) &qapla_hotcrp_cpcfn,
		&hotcrp_load_schema,
		&hotcrp_load_standard_policies,
	};
	init_query_info(&qi, DB_NAME, "/tmp/testlexer.txt", &ai);
	setup_query_in_query_info(&qi, sql, sqlen, PE_METHOD_DEFAULT);
	query_parser(&qi, sql, sqlen, 1, PE_METHOD_DEFAULT);
}

