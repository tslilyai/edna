/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __QAPLA_POLICY_POOL_H__
#define __QAPLA_POLICY_POOL_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "utils/list.h"

typedef struct {
	void *priv;
} qapla_policy_pool_t;

// == policy pool ==
qapla_policy_pool_t *init_policy_pool(uint16_t max_pol);
void free_policy_pool(qapla_policy_pool_t **pool);
void print_policy_pool(qapla_policy_pool_t *qpool, FILE *f);
void add_policy_to_pool(qapla_policy_pool_t *qpool, char *pol, int pol_len, 
		list_t *clist);
char *get_policy_from_pool(qapla_policy_pool_t *qpool, uint16_t pid);
list_t *get_policy_clist_from_pool(qapla_policy_pool_t *qpool, uint16_t pid);
uint16_t get_next_pol_id(qapla_policy_pool_t *qpool);
uint16_t get_num_policy_in_pool(qapla_policy_pool_t *qpool);

#ifdef __cplusplus
}
#endif
#endif /* __QAPLA_POLICY_POOL_H__ */
