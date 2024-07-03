// SPDX-License-Identifier: GPL-3.0

#include <linux/rhashtable.h>
#include <linux/uuid.h>
#include <linux/slab.h>

#include "hashtable.h"
#include "log.h"

const static struct rhashtable_params hashtable_object_params = {
	.key_len = sizeof(unsigned long),
	.key_offset = offsetof(struct hashtable_object, key),
	.head_offset = offsetof(struct hashtable_object, linkage),
};

void hashtable_object_free_fn(void *ptr, void *arg)
{
	START_FOLLOW;
	kvfree(ptr);
	END_FOLLOW;
}

struct hashtable *hashtable_setup(void (*free)(void *data))
{
	START_FOLLOW;
	struct hashtable *hashtable = kvmalloc(sizeof(struct hashtable), GFP_KERNEL);
	if (!hashtable) {
		log_crit("unable to allocate memory for hashtable");
		END_FOLLOW;
		return NULL;
	}
	generate_random_uuid(hashtable->id);
	hashtable->free = free;
	int ret = rhashtable_init(&hashtable->rhashtable, &hashtable_object_params);
	if (ret) {
		log_crit("unable to initialize hashtable with id '%pUB': '%d'", hashtable->id, ret);
		kvfree(hashtable);
		END_FOLLOW;
		return NULL;
	}
	END_FOLLOW;
	return hashtable;
}

int hashtable_insert(struct hashtable *hashtable, const unsigned long key, void *data)
{
	START_FOLLOW;
	struct hashtable_object *object = kvmalloc(sizeof(struct hashtable_object), GFP_KERNEL);
	if (!object) {
		log_error("unable to allocate memory for hashtable object for hashtable with id '%pUB'", hashtable->id);
		END_FOLLOW;
		return -ENOMEM;
	}
	object->data = data;
	object->key = key;
	log_debug("inserting hashtable object with key '%lu' for hashtable with id '%pUB'", key, hashtable->id);
	int ret = rhashtable_lookup_insert_fast(&hashtable->rhashtable, &object->linkage, hashtable_object_params);
	END_FOLLOW;
	return ret;
}

void *hashtable_lookup(struct hashtable *hashtable, const unsigned long key)
{
	START_FOLLOW;
	void *data = NULL;
	rcu_read_lock();
	struct hashtable_object *object = rhashtable_lookup(&hashtable->rhashtable, &key, hashtable_object_params);
	if (object) {
		data = object->data;
	} else {
		log_debug("hashtable object with key '%lu' not found for hashtable with id '%pUB'", key, hashtable->id);
	}
	rcu_read_unlock();
	END_FOLLOW;
	return data;
}

void *hashtable_delete(struct hashtable *hashtable, const unsigned long key)
{
	START_FOLLOW;
	void *ret = NULL;
	rcu_read_lock();
	struct hashtable_object *object = rhashtable_lookup(&hashtable->rhashtable, &key, hashtable_object_params);
	if (object) {
		if (!rhashtable_remove_fast(&hashtable->rhashtable, &object->linkage, hashtable_object_params)) {
			ret = object->data;
			kvfree_rcu(object, rcu_read);
			log_debug("removed hashtable object '%lu' for hashtable with id '%pUB'", key, hashtable->id);
		} else {
			log_error("unable to remove hashtable object '%lu' for hashtable with id '%pUB'", key,
				  hashtable->id);
		}
	} else {
		log_debug("hashtable object with key '%lu' not found for hashtable with id '%pUB'", key, hashtable->id);
	}
	rcu_read_unlock();
	END_FOLLOW;
	return ret;
}

void hashtable_cleanup(struct hashtable *hashtable)
{
	START_FOLLOW;
	if (hashtable->free) {
		log_debug("freeing hashtable with id '%pUB'", hashtable->id);
		struct rhashtable_iter iter;
		struct hashtable_object *object;
		rhashtable_walk_enter(&hashtable->rhashtable, &iter);
		rhashtable_walk_start(&iter);
		while ((object = rhashtable_walk_next(&iter)) != NULL) {
			if (IS_ERR(object)) {
				log_warn("found an error object while walking through hashtable with id '%pUB'",
					 hashtable->id);
				continue;
			}
			rhashtable_walk_stop(&iter);
			log_debug("freeing hashtable object with key '%lu' for hashtable with id '%pUB'", object->key,
				  hashtable->id);
			hashtable->free(object->data);
			rhashtable_walk_start(&iter);
		}
		rhashtable_walk_stop(&iter);
		rhashtable_walk_exit(&iter);
	}
	rhashtable_free_and_destroy(&hashtable->rhashtable, &hashtable_object_free_fn, NULL);
	kvfree(hashtable);
	END_FOLLOW;
}
