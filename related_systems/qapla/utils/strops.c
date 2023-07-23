/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#include <stdio.h>
#include <stdint.h>
#include <string.h>

#include "format.h"
#include "strops.h"

uint8_t
find_haystack_needle(char *haystack_start, char *haystack_end, char *needle, 
		char **match_pos)
{
	uint8_t ret = 1;
	int haystack_str_len = haystack_end - haystack_start;
	int needle_len = strlen(needle);
	if (haystack_str_len < needle_len)
		return (ret == 0);

	char *start_ptr = haystack_start;
	char *end_ptr = haystack_end;
	while (start_ptr < end_ptr - needle_len + 1) {
		ret = strncmp(start_ptr, needle, needle_len);
		if (ret == 0) {
			break;
		}

		start_ptr++;
	}

	if (ret)
		*match_pos = haystack_end;
	else
		*match_pos = start_ptr;
	return (ret == 0);
}

uint8_t
find_haystack_needle_reverse(char *haystack_start, char *haystack_end, 
		char *needle, char **match_pos)
{
	uint8_t ret = 1;
	int haystack_str_len = haystack_end - haystack_start;
	int needle_len = strlen(needle);
	if (haystack_str_len < needle_len)
		return (ret == 0);

	char *start_ptr = haystack_end - needle_len + 1;
	char *end_ptr = haystack_start;
	while (start_ptr >= end_ptr) {
		ret = strncmp(start_ptr, needle, needle_len);
		if (ret == 0)
			break;

		start_ptr--;
	}

	if (ret)
		*match_pos = haystack_start;
	else
		*match_pos = start_ptr;
	return (ret == 0);
}

uint8_t
skip_until_match_char(char *start_ptr, char *end_ptr, int fwd, int alphanum,
		char to_skip, char **new_pos)
{
	char *iter = NULL;
	uint8_t proceed = 0;
	iter = (fwd == 1 ? start_ptr : end_ptr);
	proceed = (fwd == 1 ? (iter < end_ptr) : (iter >= start_ptr));
	while (proceed) {
		if (alphanum) {
			int is_alnum = IS_ALNUM(*iter);
			if (is_alnum)
				break;
		} else {
			if (*iter != to_skip)
				break;
		}

		if (fwd)
			iter++;
		else
			iter--;
		proceed = (fwd == 1 ? (iter < end_ptr) : (iter >= start_ptr));
	}

	if ((fwd && iter < end_ptr) || iter >= start_ptr) {
		*new_pos = iter;
		return 1;
	}

	*new_pos = NULL;
	return 0;
}

uint8_t
skip_until_unmatch_char(char *start_ptr, char *end_ptr, int fwd, int alphanum,
		char to_skip, char **new_pos)
{
	char *iter = NULL;
	uint8_t proceed = 0;
	iter = (fwd == 1 ? start_ptr : end_ptr);
	proceed = (fwd == 1 ? (iter < end_ptr) : (iter >= start_ptr));
	while (proceed) {
		if (alphanum) {
			int is_alnum = IS_VALID_IDENTIFIER_CHAR(*iter);
			if (!is_alnum)
				break;
		} else {
			if (*iter == to_skip)
				break;
		}

		if (fwd)
			iter++;
		else
			iter--;
		proceed = (fwd == 1 ? (iter < end_ptr) : (iter >= start_ptr));
	}

	if ((fwd && iter < end_ptr) || iter >= start_ptr) {
		*new_pos = iter;
		return 1;
	}

	*new_pos = NULL;
	return 0;
}

