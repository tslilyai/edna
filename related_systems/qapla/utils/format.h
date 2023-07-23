/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __FORMAT_H__
#define __FORMAT_H__

#include <stdint.h>

#define MIN_DIGIT_ASCII_VAL	48
#define IS_ALNUM(c)	\
	((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||	\
			(c - MIN_DIGIT_ASCII_VAL >= 0 && c - MIN_DIGIT_ASCII_VAL <= 9))

#define IS_VALID_IDENTIFIER_CHAR(c)	\
	IS_ALNUM(c) || (c == '_')

// ptr in range [start, end)
#define IS_VALID_PTR(ptr, start, end)	\
	((void *)(ptr) &&	\
	 ((void *)(ptr) >= (void *)(start)) && ((void *)(ptr) < (void *)(end)))

#ifdef __cplusplus
extern "C" {
#endif

	// out_len = 2*in_len+1;
	void convert_str_to_hex(char *in, int in_len, char *out);
	// out_len = (in_len-1)/2;
	void convert_hex_to_str(char *in, int in_len, char *out);
	void convert_binary_to_ascii_str(uint8_t *in, int n_in, char *out);
	void convert_ascii_str_to_binary(char *in, int in_len, uint8_t *out);

	uint8_t is_valid_integer(char *start, char *end);

#ifdef __cplusplus
}
#endif

#endif /* __FORMAT_H__ */
