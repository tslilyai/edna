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

	bool topic_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
	void topic_deinit(UDF_INIT *initid);
	char *topic(UDF_INIT *initid, UDF_ARGS *args, char *result, 
			unsigned long *length, char *is_null, char *error);
#ifdef __cplusplus
}
#endif

#define MAX_AFFIL_NAMELEN	64

bool 
topic_init(UDF_INIT *initid, UDF_ARGS *args, char *message)
{
	if (args->arg_count != 1 || args->arg_type[0] != STRING_RESULT) {
		strcpy(message, "Wrong arguments to topic, use authorInformation column");
		return 1;
	}
	initid->max_length=MAX_AFFIL_NAMELEN;
	
	int fd = open("/tmp/topic_file.txt", O_RDONLY);
	if (fd < 0) {
		strcpy(message, "Can't open topic_file.txt");
		return 1;
	}

	initid->ptr = (char *) malloc(sizeof(int));
	*(int *) initid->ptr = fd;

	return 0;
}

void
topic_deinit(UDF_INIT *initid)
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
topic(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length,
		char *is_null, char *error)
{
	const char *word = args->args[0];
	int word_len = args->lengths[0];
	char *word_end = (char *) word + word_len;

	char pattern1 = ' ', pattern2 = '\n';
	char *next = NULL;
	char *topic_start, *topic_end;
	topic_start = (char *) word;
	get_char_pos((char *) word, word_end, &pattern1, &next);
	if (!next || next >= word_end) {
		*length = 0;
		return NULL;
	}
	topic_end = next;
	if (topic_start < topic_end && topic_end < word_end) {
		strncpy(result, topic_start, topic_end - topic_start);
		*length = topic_end - topic_start;
	} else {
		*length = 0;
		return NULL;
	}

	return result;
}

