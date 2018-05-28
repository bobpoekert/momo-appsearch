#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "queue.c"
#include "murmur3.c"
#include "main.h"

/*
 * fan the input lines out to worker threads
 * each worker thread generates a binary search tree keyed on hash with count
 * when done, lazily merge trees and write into an mtbl index
 *
 */

void flush_strings(worker_ctx *ctx) {
    write(ctx->strings_fd, ctx->strings_buffer, ctx->strings_buffer_offset);
    ctx->strings_buffer_offset = 0;
}

size_t add_string(worker_ctx *ctx, char *string, size_t string_length) {
    size_t offset = ctx->strings_offset;
    if (CHUNK_SIZE - ctx->strings_buffer_offset < string_length) {
        flush_strings(ctx);
    }
    memcpy(ctx->strings_buffer + ctx->strings_buffer_offset, string, string_length);
    ctx->strings_buffer_offset += string_legnth;
    ctx->strings_offset += string_length;
    return offset;
}


void populate_string_row(worker_ctx *ctx, string_row *row, char *string, size_t string_length) {
    size_t offset = add_string(ctx, string, string_length);
    row->file_offset = offset;
}

string_tree *string_tree_alloc() {
    string_tree *res = malloc(sizeof(string_tree));
    memset(res, 0, sizeof(string_tree));
    return res;
}

void string_tree_free(string_tree *h) {
    free(h);
}

inline bool string_tree_is_empty(string_tree *h, size_t idx) {
    return h->heap[idx]->count == 0;
}

string_tree_node *string_tree_node_lookup(worker_ctx *ctx, string_tree *h, string_row *row) {
    if (h->length < 1) return 0;
    string_tree_node *current_node = h->nodes;
    while (1) {
        string_tree_node *parent = current_node;
        char cmp = hash_compare(current_node->row->hash, row->hash);
        if (cmp == 0) {
            return current_node;
        } else if (cmp < 0) {
            current_node = current_node->left;
        } else {
            current_node = current_node->right;
        }
        if (current_node == 0) return parent;
    }
}

void string_tree_insert(worker_ctx *ctx, string_tree *h, string_row *row, char *string, size_t string_length) {
    /* caller is responsible for doing overflow checking */
    string_tree_node *parent = string_tree_node_lookup(h, row);
    if (parent == 0) {
        /* tree is empty */
        string_tree_node *target = h->nodes[0];
        memcpy(target->row, row, sizeof(string_row));
        return;
    }
    char cmp = hash_compare(row->hash, parent->row->hash);
    if (cmp == 0) {
        parent->row->count++;
    } else {
        string_tree_node *target = h->nodes[h->length];
        populate_string_row(ctx, row, string, string_length);
        memcpy(target->row, row, sizeof(string_row));
        h->length++;
        if (cmp < 0) {
            parent->left = target;
        } else {
            parent->right = target;
        }
    }
}

int indexof(char *string, char target, size_t max_length) {
    size_t offset = 0;
    while (offset < max_length) {
        if (string[offset] == target) return offset;
        offset++;
    }
    return -1;
}

void worker(void *vctx) {
    worker_ctx *ctx = vctx;
    queue *inq = ctx->inq;
    while (1) {
        job *j = queue_take(inq);
        if (j == NULL) { /* work done, no more jobs */
            queue_put(inq, NULL);
            break;
        }
        size_t line_offset = 0;
        size_t max_length = j->buf_size;
        while (max_length > 0) {
            char *line = j->buf + line_offset;
            int line_end = indexof(line, '\n', max_length);
            if (line_end < 0) break;
            max_length -= line_end;
            line_offset += line_end;
            string_row row;
            row.count = 1;
            row.file_id = ctx->strings_fd;
            murmur_hash(line, line_end, &row.hash);

            if (TREE_SIZE - ctx->tail->length < 1) {
                string_tree *new_tree = malloc(sizeof(string_tree));
                new_tree->length = 0;
                new_tree->next = NULL;
                ctx->tail->next = new_tree;
                ctx->tail = new_tree;
            }

            string_tree_insert(ctx, ctx->head, &row, line, (size_t) line_end);
        }
        queue_put(ctx->pool, j);
    }
    flush_strings(ctx);
}

void input_reader(void *ctx) {
    queue *outq = ((inserter_ctx *) ctx)->outq;
    queue *pool = ((inserter_ctx *) ctx)->pool;
    size_t overflow_size = 0;
    char *overflow = malloc(CHUNK_SIZE);
    while (1) {
        job *j = queue_take(pool);
        char *buf = j->buf;
        if (buf == 0) continue;
        if (overflow_size > 0) {
            memcpy(buf, overflow, overflow_size);
        }
        if (read(0, buf + overflow_size, CHUNK_SIZE - overflow_size) == EOF) break;
        overflow_size = 0;
        while (overflow_size < CHUNK_SIZE) {
            char c = buf[buffer_size - overflow_size];
            if (c == '\n') break;
            overflow_size++;
        }
        if (overflow_size > 0) {
            memcpy(overflow, buf + (CHUNK_SIZE - overflow_size), overflow_size);
        }
        j->buf_size = CHUNK_SIZE - overflow_size;
        queue_put(outq, j);
    }
    queue_put(outq, NULL);
    free(overflow);
    queue_free(outq);
    queue_free(pool);
    free(ctx);
}

int main(int argc, char **argv) {

    size_t thread_count;

    if (argc != 2) {
        printf("%s: <thread count>", argv[0]);
        return 1;
    }

    thread_count = atol(argv[1]);

    queue *q = queue_alloc(256);
    pthread_t *inp_thread;
    pthread_create(&inp_thread, NULL, input_reader, (void *) q);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    pthread_t **worker_threads = malloc(sizeof(pthread_t *) * thread_count);
    worker_ctx *worker_contexts = malloc(sizeof(worker_ctx) * thread_count);
    memset(worker_contexts, 0, sizeof(worker_ctx) * thread_count);
   
    for (size_t thread_id = 0; thread_id < thread_count; thread_id++) {
        worker_ctx *ctx = worker_contexts[i];
        ctx->inq = q;
        pthread_create(worker_threads[thread_id], &attr, worker, (void *) ctx);
    }

    for (size_t thread_id = 0; thread_id < thread_count; thread_id++) {
        void *staus;
        pthread_join(worker_threads[i], &status);
    }

    pthread_exit(NULL);
    return 0;
}
