#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TOMBSTONE ((char *)0x1)

typedef struct {
    char *key;
    char *value;
} kv_entry_t;

typedef struct {
    kv_entry_t *entries;
    size_t      capacity;
    size_t      count;
} kv_t;

static size_t hash(const char *key, size_t capacity) {
    size_t h = 14695981039346656037ULL; /* FNV-1a offset basis */
    while (*key) {
        h ^= (unsigned char)*key++;
        h *= 1099511628211ULL;          /* FNV prime */
    }
    return h % capacity;
}

kv_t *kv_init(size_t capacity) {
    kv_t *db = malloc(sizeof(kv_t));
    if (!db) return NULL;

    db->entries = calloc(capacity, sizeof(kv_entry_t));
    if (!db->entries) { free(db); return NULL; }

    db->capacity = capacity;
    db->count    = 0;
    return db;
}

int kv_put(kv_t *db, const char *key, const char *value) {
    if (!db || !key || !value) return -1;
    if (db->count >= db->capacity / 2) return -1; /* load factor exceeded */

    size_t idx = hash(key, db->capacity);

    for (size_t i = 0; i < db->capacity; i++) {
        kv_entry_t *e = &db->entries[(idx + i) % db->capacity];

        /* update existing key */
        if (e->key && e->key != TOMBSTONE && strcmp(e->key, key) == 0) {
            char *newval = strdup(value);
            if (!newval) return -1;
            free(e->value);
            e->value = newval;
            return 0;
        }

        /* empty or tombstone slot */
        if (!e->key || e->key == TOMBSTONE) {
            char *newkey = strdup(key);
            char *newval = strdup(value);
            if (!newkey || !newval) { free(newkey); free(newval); return -1; }
            e->key   = newkey;
            e->value = newval;
            db->count++;
            return 0;
        }
    }

    return -1; /* table full */
}

char *kv_get(kv_t *db, const char *key) {
    if (!db || !key) return NULL;

    size_t idx = hash(key, db->capacity);

    for (size_t i = 0; i < db->capacity; i++) {
        kv_entry_t *e = &db->entries[(idx + i) % db->capacity];

        if (!e->key) return NULL; /* empty slot, key not present */
        if (e->key != TOMBSTONE && strcmp(e->key, key) == 0)
            return e->value;
    }

    return NULL;
}

int kv_delete(kv_t *db, const char *key) {
    if (!db || !key) return -1;

    size_t idx = hash(key, db->capacity);

    for (size_t i = 0; i < db->capacity; i++) {
        kv_entry_t *e = &db->entries[(idx + i) % db->capacity];

        if (!e->key) return -1; /* not found */
        if (e->key != TOMBSTONE && strcmp(e->key, key) == 0) {
            free(e->key);
            free(e->value);
            e->key   = TOMBSTONE;
            e->value = NULL;
            db->count--;
            return 0;
        }
    }

    return -1;
}

void kv_free(kv_t *db) {
    if (!db) return;
    for (size_t i = 0; i < db->capacity; i++) {
        kv_entry_t *e = &db->entries[i];
        if (e->key && e->key != TOMBSTONE) {
            free(e->key);
            free(e->value);
        }
    }
    free(db->entries);
    free(db);
}
