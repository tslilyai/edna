/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __SYM_H__
#define __SYM_H__

#ifdef __cplusplus
extern "C" {
#endif
	typedef struct sym {
		int type;
		int id;
		int db_tid;
		int db_cid;
		char *name;
		char *db_tname;
		char *db_cname;
	} sym_t;

	enum {
		SYM_TAB = 0,
		SYM_COL,
		SYM_LAST
	};

#define NUM_SYM_TYPE SYM_LAST

#define set_sym_field(sym, field, val)	\
	sym->s.field = (val)

#define set_sym_field_from_src(sym, field, src)	\
	sym->s.field = src->s.field

#define set_sym_str_field(sym, field, val)	\
	do {	\
		if (val)	\
			sym->s.field = strdup(val);	\
	} while(0)

#define set_sym_str_field_from_src(sym, field, src)	\
	do {	\
		if (src->s.field)	\
			sym->s.field = strdup(src->s.field);	\
	} while(0)

#define free_sym_str_field(sym, field)	\
	do {	\
		if (sym->s.field)	\
			free(sym->s.field);	\
		sym->s.field = NULL;	\
	} while (0)

#define set_sym_ptr_field(sym_p, field, val)	\
	(sym_p)->field = (val)

#define sym_field(sym, field)	sym->s.field
#define sym_ptr_field(sym_p, field)	(sym_p->s)->field

#ifdef __cplusplus
}
#endif

#endif /* __SYM_H__ */
