#include "mcdb.h"
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

typedef struct db_ctx {
    struct mcdb db;
    struct mcdb_mmap map;
    struct mcdb_iter it;
} db_ctx;

void *db_init(int fd) {
    db_ctx *res = malloc(sizeof(db_ctx));
    memset(res, 0, sizeof(db_ctx));
    (res->db).map = &(res->map);
    if (!mcdb_mmap_init(&(res->map), fd)) {
        free(res);
        return 0;
    }
    mcdb_iter_init(&(res->it), &(res->db));
    return res;
}

void db_free(db_ctx *ctx) {
    mcdb_mmap_free(&(ctx->map));
    free(ctx);
}

char db_next(db_ctx *ctx, char **k, size_t *ksize, char **v, size_t *vsize) {
    struct mcdb_iter *it = &(ctx->it);
    if (!mcdb_iter(it)) return 0;
    *ksize = mcdb_iter_keylen(it);
    *k = mcdb_iter_keyptr(it);
    *vsize = mcdb_iter_datalen(it);
    *v = mcdb_iter_dataptr(it);
    return 1;
}

char db_int_key_next(db_ctx *ctx, uint32_t *k, char **v, size_t *vsize) {
    char *kk;
    size_t ksize;
    if (!db_next(ctx, &kk, &ksize, v, vsize)) return 0;
    if (ksize != sizeof(uint32_t)) return 0;
    *k = *kk;
    return 1;
}
