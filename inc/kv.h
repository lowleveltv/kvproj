#ifndef KV_H
#define KV_H

#include <stddef.h>

typedef struct {
    char *key;
    char *value;
} kv_entry_t;

typedef struct {
    kv_entry_t *entries;
    size_t      capacity;
    size_t      count;
} kv_t;

kv_t  *kv_init(size_t capacity);
int    kv_put(kv_t *db, const char *key, const char *value);
char  *kv_get(kv_t *db, const char *key);
int    kv_delete(kv_t *db, const char *key);
void   kv_free(kv_t *db);

#endif /* KV_H */
