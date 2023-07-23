/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#include <mysql.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define MYSQL_SERVER 1

#ifdef __cplusplus
extern "C" {
#endif

	bool affil_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
	void affil_deinit(UDF_INIT *initid);
	char *affil(UDF_INIT *initid, UDF_ARGS *args, char *result, 
			unsigned long *length, char *is_null, char *error);
#ifdef __cplusplus
}
#endif

#define MAX_AFFIL_NAMELEN	64

bool 
affil_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT) {
		strcpy(message, "Wrong arguments to affil, use authorInformation column");
		return 1;
	}
	initid->max_length=MAX_AFFIL_NAMELEN;
	
	int fd = open("/tmp/affil_file.txt", O_RDONLY);
	if (fd < 0) {
		strcpy(message, "Can't open affil_file.txt");
		return 1;
	}

	initid->ptr = (char *) malloc(sizeof(int));
	*(int *) initid->ptr = fd;

	return 0;
}

void
affil_deinit(UDF_INIT *initid)
{
	int fd = *(int *)initid->ptr;
	close(fd);
}

static void
get_char_pos(char *str, char *end, char *pattern, char **next)
{
	if (!next)
		return;

	char *c;
	if (*next)
		c = *next;
	else
		c = str;

	while (c < end) {
		if (*c == *pattern) {
			*next = c;
			return;
		}
		c++;
	}
	*next = end;
}

char *
affil(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length,
		char *is_null, char *error)
{
	const char *word = args->args[0];
	int word_len = args->lengths[0];
	char *word_end = (char *) word + word_len;

	char pattern1 = '\t', pattern2 = '\n';
	char *next = NULL;
	char *affil_start, *affil_end;
	// first name end
	get_char_pos((char *) word, word_end, &pattern1, &next);
	if (!next || next >= word_end) {
		*length = 0;
		return NULL;
	}
	next++;
	// last name end
	get_char_pos((char *) word, word_end, &pattern1, &next);
	if (!next || next >= word_end) {
		*length = 0;
		return NULL;
	}
	next++;
	// email end
	get_char_pos((char *) word, word_end, &pattern1, &next);
	if (!next || next >= word_end) {
		*length = 0;
		return NULL;
	}
	next++;
	affil_start = next;
	get_char_pos((char *) word, word_end, &pattern2, &next);
	if (!next || next >= word_end) {
		*length = 0;
		return NULL;
	}
	affil_end = next;
	if (affil_start < affil_end && affil_end < word_end) {
		strncpy(result, affil_start, affil_end - affil_start);
		*length = affil_end - affil_start;
	} else {
		*length = 0;
		return NULL;
	}

	return result;
}

