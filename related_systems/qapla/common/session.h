/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __DLOG_SESSION_H__
#define __DLOG_SESSION_H__

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SS_SUCCESS 0
#define SS_FAILURE 1

typedef struct session {
	void *priv;
} session_t;

uint32_t init_session(session_t *session, char *email, uint32_t ip);
uint32_t cleanup_session(session_t *session);
char *get_session_email(session_t *session);
uint32_t get_session_ip(session_t *session);

#ifdef __cplusplus
}
#endif

#endif /* __DLOG_SESSION_H__ */
