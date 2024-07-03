// SPDX-License-Identifier: GPL-3.0

#ifndef SILO_HASHTABLE_H
#define SILO_HASHTABLE_H

#ifndef UUID_SIZE
#define UUID_SIZE 16
#endif

#include <linux/rhashtable.h>
#include <linux/uuid.h>

struct hashtable_object {
	unsigned long key;
	struct rhash_head linkage;
	void *data;
	struct rcu_head rcu_read;
};

struct hashtable {
	unsigned char id[UUID_SIZE];
	struct rhashtable rhashtable;
	void (*free)(void *ptr);
};

struct hashtable *hashtable_setup(void (*free)(void *ptr));
int hashtable_insert(struct hashtable *hashtable, const unsigned long key, void *data);
void *hashtable_lookup(struct hashtable *hashtable, const unsigned long key);
void *hashtable_delete(struct hashtable *hashtable, const unsigned long key);
void hashtable_cleanup(struct hashtable *hashtable);

#endif //SILO_HASHTABLE_H
