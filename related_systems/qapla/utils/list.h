/*
 * **************************************************************
 * Copyright (c) 2017-2022, Aastha Mehta <aasthakm@mpi-sws.org>
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found
 * in the LICENSE file in the root directory of this source tree.
 * **************************************************************
 */
#ifndef __LIST_H__
#define __LIST_H__

#ifdef __cplusplus
extern "C" {
#endif

#ifndef offsetof
#define offsetof(type, member)	((size_t) &((type *)0)->member)
#endif

#define container_of(ptr, type, member)		\
	({\
		const typeof( ((type *) 0)->member) * __ptr = (ptr);	\
		(type *)( (char *)__ptr - offsetof(type, member) );	\
	 })

#define list_first_entry(list, type, member)	\
	list_entry((list)->next, type, member)

#define list_entry(ptr, type, member)	\
	container_of(ptr, type, member)

#define list_next_entry(pos, member)	\
	list_entry((pos)->member.next, typeof(*(pos)), member)

#define list_for_each_entry(ptr, head, member)	\
	for (ptr = list_first_entry(head, typeof(*ptr), member);	\
			&ptr->member != (head);	\
			ptr = list_next_entry(ptr, member))

struct list;

typedef struct list {
	struct list *next;
	struct list *prev;
} list_t;

static inline void list_init(struct list *list)
{
	list->next = list;
	list->prev = list;
}

static inline void list_insert(struct list *list, struct list *elem)
{
	elem->next = list->next;
	elem->prev = list;
	elem->next->prev = elem;
	elem->prev->next = elem;
}

static inline void list_migrate(struct list *dst_list, struct list *src_list)
{
	src_list->next->prev = dst_list;
	src_list->prev->next = dst_list;
	dst_list->next = src_list->next;
	dst_list->prev = src_list->prev;
	src_list->next = src_list;
	src_list->prev = src_list;
}

static inline void list_insert_at_tail(struct list *list, struct list *elem)
{
	elem->next = list;
	elem->prev = list->prev;
	elem->next->prev = elem;
	elem->prev->next = elem;
}

static inline void list_remove(struct list *elem)
{
	elem->next->prev = elem->prev;
	elem->prev->next = elem->next;
	elem->prev = elem;
	elem->next = elem;
}

static inline int list_empty(struct list *elem)
{
	if (!elem)
		return 1;

	if (elem->next == elem && elem->prev == elem)
		return 1;

	return 0;
}

#ifdef __cplusplus
}
#endif

#endif /* __LIST_H__ */
