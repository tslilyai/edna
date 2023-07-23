#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#include "common/config.h"
#include "qapla.h"
#include "websubmit/websubmit_pol_db.h"
#include "websubmit/websubmit_db.h"

#define MAX_BUF_LEN	1024
#define ROLE_CHAIR 4

#ifdef __cplusplus
extern "C" {
#endif

extern void rewrite_query(char* email, char* sql, char** rewrite_q, bool cellblind);

#ifdef __cplusplus
}
#endif


