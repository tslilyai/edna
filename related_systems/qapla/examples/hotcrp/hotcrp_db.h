#ifndef __HOTCRP_DB_H__
#define __HOTCRP_DB_H__

#include "common/db.h"
#include "qinfo/metadata.h"

#ifdef __cplusplus
extern "C" {
#endif

extern int hotcrp_load_schema(db_t *db);
extern void hotcrp_load_standard_policies(db_t *schema, metadata_t *qm,
		void *sql_pred_fns, void *pol_fns, void *col_fns);

#ifdef __cplusplus
}
#endif

#endif /* __HOTCRP_DB_H__ */
