#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "util.h"
#include "libsstable.h"


SSTableCtx *SSTable_open(
        char *bin_name, size_t bin_name_size,
        char *text_name, size_t text_name_size) {

    struct stat sb;
    SSTableCtx *res = malloc(sizeof(SSTableCtx));
    memset(res, 0, sizeof(SSTableCtx));


    res->bin_fd = open(bin_name, O_RDONLY);
    if (res->bin_fd == -1) {
        goto fail;
    }
    res->text_fd = open(text_name, O_RDONLY);
    if (res->text_fd == -1) {
        goto fail;
    }


    if (fstat(res->bin_fd, &sb) == -1) {
        goto fail;
    }
    res->bin_size = sb.st_size;
    if (fstat(res->text_fd, &sb) == -1) {
        goto fail;
    }
    res->text_size = sb.st_size;

    res->bin_buf_head = mmap(
            NULL, res->bin_size,
            PROT_READ, MAP_PRIVATE,
            res->bin_fd, 0);
    if (res->bin_buf_head == MAP_FAILED) {
        goto fail;
    }
    res->text_buf_head = mmap(
            NULL, res->text_size,
            PROT_READ, MAP_PRIVATE,
            res->text_fd, 0);
    if (res->text_buf_head == MAP_FAILED) {
        goto fail;
    }

    return res;

fail:
    SSTable_close(res);
    return 0;

}

int SSTable_close(SSTableCtx *ctx) {
    int status = 0;
    if (ctx == NULL) return -1;

    if (ctx->bin_buf_head != 0 && ctx->bin_buf_head != MAP_FAILED) {
        if (munmap(ctx->bin_buf_head, ctx->bin_size) == -1) status = -1;
    }
    if (ctx->text_buf_head != 0 && ctx->text_buf_head != MAP_FAILED) {
        if (munmap(ctx->text_buf_head, ctx->text_size) == -1) status = -1;
    }

    if (ctx->bin_fd != 0) {
        close(ctx->bin_fd);
    }
    if (ctx->text_fd != 0) {
        close(ctx->text_fd);
    }

    free(ctx);
    return status;

}

static uint64_t *SSTable_hashes(SSTableCtx *ctx) {
    return (uint64_t *) ctx->bin_buf_head;
}

static size_t SSTable_n_keys(SSTableCtx *ctx) {
    return ctx->bin_size / sizeof(uint64_t) / 2;
}

static uint64_t *SSTable_offsets(SSTableCtx *ctx) {
    return (uint64_t *) (ctx->bin_buf_head + SSTable_n_keys(ctx) * sizeof(uint64_t));
}

int SSTable_lookup_hash(SSTableCtx *ctx,
        uint64_t k, char **res, size_t *res_size) {

    uint64_t *hashes = SSTable_hashes(ctx);
    uint64_t *offsets = SSTable_offsets(ctx);
    size_t n_keys = SSTable_n_keys(ctx);

    uint64_t pivot = 0;
    uint64_t pivot_idx;
    uint64_t left = 0;
    uint64_t right = n_keys;

    while(right > left) {
        pivot_idx = left + (right - left) / 2;
        pivot = hashes[pivot_idx];

        if (pivot == k) {
            break;
        } else if (pivot > k) {
            right = pivot_idx;
        } else {
            left = pivot_idx;
        }

    }

    if (pivot != k) return -1;

    size_t byte_offset = offsets[pivot_idx];
    
    if (pivot_idx < n_keys-1) {
        *res_size = ctx->text_size - byte_offset;
    } else {
        *res_size = offsets[pivot_idx + 1] - byte_offset;
    }

    *res = &(ctx->text_buf_head[byte_offset]);

    return 0;

}

int SSTable_lookup_key(SSTableCtx *ctx,
        char *key, size_t key_size,
        char **res, size_t *res_size) {
    uint64_t k_hash = hash_bytes(key, key_size);
    return SSTable_lookup_hash(ctx, k_hash, res, res_size);
}
