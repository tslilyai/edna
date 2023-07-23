#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#include "common/config.h"
#include "qapla.h"
#include "refmon_ws_lib.h"

#define MAX_BUF_LEN	1024

#define ROLE_CHAIR 4

int main(int argc, char *argv[])
{
	if (argc < 2) {
		printf("Usage: ./exe <sql-string>\n");
		printf("E.g.: ./examples/refmon \"select * from A;\"\n");
		return -1;
	}

	char *sql = argv[1];
	int sqlen = strlen(sql);

	char *dbgfile = "tmp/refmon_ws.dbg";
	char *outfile = "tmp/sql_ws.txt";

	printf("rewriting %s\n", sql);

	char *rewrite_q = NULL;
	printf("Going to rewrite query! %p\n", &rewrite_q);
	rewrite_query("example@email.com", sql, &rewrite_q, true);
	printf("refmon %s at %p\n", rewrite_q, &rewrite_q);

	// dump rewritten query to a file
	FILE *f = fopen(outfile, "a+");
	if (f) {
		//fprintf(f, "use %s;\n", DB_NAME);
		fprintf(f, "%s\n", rewrite_q);
		printf("%s\n", rewrite_q);
		fclose(f);
	}

	return EXIT_SUCCESS;
}
