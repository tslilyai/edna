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
#include <stdlib.h>
#include <string.h>

#include "MurmurHash3.h"

#define HASH_SEED 7

int
murmur_hash(void *key, int key_len, int modulo)
{
	uint64_t idx[2] = {0, 0};
	MurmurHash3_x64_128((const void *) key, key_len, HASH_SEED, (uint64_t *) idx);
	return (int) (idx[0] % modulo);
}

int
int_index(void *key, int key_len, int modulo)
{
	int k = *(int *) key;
	return k % modulo;
}
