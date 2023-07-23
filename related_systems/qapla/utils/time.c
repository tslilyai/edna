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
#include <time.h>
#include "time.h"

uint64_t
get_timestamp(char *datetime)
{
	struct tm tm;
	time_t epoch = 0;
	memset(&tm, 0, sizeof(struct tm));
	if (strptime(datetime, "%Y-%m-%d %H:%M:%S", &tm) != NULL)
		epoch = mktime(&tm);

	return (uint64_t) epoch;
}

