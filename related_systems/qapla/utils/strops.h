/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __STROPS_H__
#define __STROPS_H__

#ifdef __cplusplus
extern "C" {
#endif

	uint8_t find_haystack_needle(char *haystack_start, char *haystack_end,
			char *needle, char **match_pos);
	uint8_t find_haystack_needle_reverse(char *haystack_start, char *haystack_end,
			char *needle, char **match_pos);
	uint8_t skip_until_match_char(char *start_ptr, char *end_ptr, int fwd,
			int alphanum, char to_skip, char **new_pos);
	uint8_t skip_until_unmatch_char(char *start_ptr, char *end_ptr, int fwd,
			int alphanum, char to_skip, char **new_pos);

#ifdef __cplusplus
}
#endif

#endif /* __STROPS_H__ */
