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
#include <stdint.h>

#include "session.h"

typedef enum session_state {
	SS_UNUSED = 0,
	SS_LINKED
} dlog_sess_state_t;

typedef struct sess_int {
	uint32_t sess_id;
	dlog_sess_state_t state;
	char *email;
	uint32_t ip;
} session_int_t;

uint32_t
init_session(session_t *session, char *email, uint32_t ip)
{
	session_t *s = (session_t *) session;
	if (!s)
		return SS_FAILURE;

	session_int_t *s_priv; 
	s->priv = s_priv = (session_int_t *) malloc(sizeof(session_int_t));
	memset(s_priv, 0, sizeof(session_int_t));
	s_priv->email = strdup(email);
	s_priv->ip = ip;
	s_priv->state = SS_LINKED;

	return SS_SUCCESS;
}

uint32_t
cleanup_session(session_t *session)
{
	session_int_t *s_priv = (session_int_t *) session->priv;
	if (!s_priv)
		return SS_SUCCESS;

	if (s_priv->email)
		free(s_priv->email);
	free(s_priv);
	session->priv = NULL;

	return SS_SUCCESS;
}

char *
get_session_email(session_t *session)
{
	session_int_t *s_priv = (session_int_t *) session->priv;
	return s_priv->email;
}

uint32_t
get_session_ip(session_t *session)
{
	session_int_t *s_priv = (session_int_t *) session->priv;
	return s_priv->ip;
}
