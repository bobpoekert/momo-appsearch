#pragma once
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

typedef struct SSTableCtx {
    int bin_fd;
    char *bin_buf_head;
    int text_fd;
    char *text_buf_head;
    size_t bin_size;
    size_t text_size;
} SSTableCtx;


SSTableCtx *SSTable_open(
        char *bin_name, size_t bin_name_size,
        char *text_name, size_t text_name_size);
int SSTable_close(SSTableCtx *ctx);
int SSTable_lookup_hash(SSTableCtx *ctx, uint64_t k, char **res, size_t *res_size);
int SSTable_lookup_key(SSTableCtx *ctx,
        char *key, size_t key_size,
        char **res, size_t *res_size);
