/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __TIME_H__
#define __TIME_H__

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
	uint64_t get_timestamp(char *datetime);

#ifdef __cplusplus
}
#endif

#endif /* __TIME_H__ */
