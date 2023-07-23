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
#include <string.h>

#include "format.h"

// caller must allocate out buf of out_len = 2*in_len+1;
void
convert_str_to_hex(char *in, int in_len, char *out)
{
	if (!in || !in_len || !out)
		return;

	int len = 2*(in_len+1);
	memset(out, 0, len);

	int i;
	for (i = 0; i < in_len; i++) {
		sprintf(&out[2*i], "%02x", (unsigned int)in[i]);
	}
	//printf("in[0], char: %c, %d, int: %d\n", in[0], (unsigned int)in[0], 
	//		((unsigned int *)in)[0]);
	//printf("orig(%d): %s\nhex buf(%d): %s\n", in_len, in, len, out);
}

// caller must allocate out buf of out_len = (in_len-1)/2;
void
convert_hex_to_str(char *in, int in_len, char *out)
{
	if (!in || !in_len || !out)
		return;

	int len = (in_len-1)/2;
	memset(out, 0, len);

	int i,j;
	char *ptr;
	for (i = 0, j = 0; i < len; i++,j+=2) {
		int d;
		ptr = &in[j];
		sscanf(ptr, "%02x", &d);
		//printf("scanned char: %d, %c\n", d, (char)d);
		sprintf(&out[i], "%c", (char)d);
	}
	//printf("out(%d): %s\n", len, out);
}

// n array of bytes; caller must alloc out of size (n_in*8);
void
convert_binary_to_ascii_str(uint8_t *in, int n_in, char *out)
{
	int i, j;
	uint8_t it, digit;
	char c;
	char *ptr = out;
	for (i = 0; i < n_in; i++) {
		it = in[i];
		for (j = 0; j < 8; j++) {
			digit = (it & (1 << j)) >> j;
			c = digit + MIN_DIGIT_ASCII_VAL;
			sprintf(ptr, "%c", c);
			ptr++;
		}
	}
}

// caller must alloc out of in_len/8 to hold all int8 bytes
void
convert_ascii_str_to_binary(char *in, int in_len, uint8_t *out)
{
	int i, j, out_it = 0;
	char c;
	uint8_t digit;
	for (i = 0; i < in_len; i++) {
		for (j = 0; j < 8; j++) {
			c = in[i+j];
			digit = c - MIN_DIGIT_ASCII_VAL;
			if (digit >= 0 && digit <= 1) // should only be 0 or 1
				out[out_it] |= (digit << j);
		}
		i += (j-1);
		out_it++;
	}
}

uint8_t
is_valid_integer(char *start, char *end)
{
	char *ptr = start;
	uint8_t digit = 0;
	uint8_t ret = 0;

	if (start >= end)
		return ret;

	while (ptr < end) {
		digit = *ptr - MIN_DIGIT_ASCII_VAL;
		if (!(digit >= 0 && digit <= 9))
			return ret;

		ptr++;
	}

	return 1;
}
