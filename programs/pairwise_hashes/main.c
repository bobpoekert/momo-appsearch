#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>

#include "util.h"

#define MIN(a,b) (a > b ? b : a)
#define MAX(a,b) (a > b ? a : b)

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#define ASSERT(v, msg) if (unlikely(!(v))) { printf(msg); abort(); }

#define TREE_CHUNK_SIZE 4096000
#define VAL_GROUP_SIZE 40960

void *mmap_read_file(char *fname) {
    struct stat file_info;
    if (stat(fname, &file_info) == -1) return NULL;

    int fd = open(fname, O_RDONLY);

    void *res = mmap(NULL, file_info.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (res == MAP_FAILED) {
        perror("mmap");
        return NULL;
    }

    return res;
}

static pthread_mutex_t printf_mutex;

void sync_printf(const char *format, ...) {
    va_list args;
    va_start(args, format);

    pthread_mutex_lock(&printf_mutex);
    vprintf(format, args);
    pthread_mutex_unlock(&printf_mutex);

    va_end(args);
}

typedef struct HashPair {
    uint64_t left;
    uint64_t right;
} HashPair;

typedef struct TreeNode {

    uint64_t key_left;
    uint64_t key_right;
    uint64_t value;
    struct TreeNode *left;
    struct TreeNode *right;

} TreeNode;

typedef struct TreeChunk {

    void *data;
    size_t total_size;
    size_t offset;
    struct TreeChunk *next;

} TreeChunk;

typedef struct Tree {

    TreeNode *root;
    TreeChunk *head_chunk;
    TreeChunk *tail_chunk;
    size_t n_nodes;

} Tree;

TreeChunk *TreeChunk_alloc() {
    void *new_chunk_buf = malloc(TREE_CHUNK_SIZE + sizeof(TreeChunk));
    TreeChunk *new_chunk = (TreeChunk *) new_chunk_buf;
    new_chunk->data = new_chunk_buf + sizeof(TreeChunk);
    new_chunk->total_size = TREE_CHUNK_SIZE;
    new_chunk->offset = 0;
    new_chunk->next = NULL;
    return new_chunk;
}

Tree *Tree_alloc() {
    Tree *res = (Tree *) malloc(sizeof(Tree));
    res->head_chunk = TreeChunk_alloc();
    res->tail_chunk = res->head_chunk;
    res->n_nodes = 0;
    res->root = NULL;
    return res;
}

void Tree_free(Tree *tree) {
    TreeChunk *cur_chunk = NULL;
    while (cur_chunk != tree->tail_chunk) {
        TreeChunk *prev_chunk = cur_chunk;
        if (prev_chunk != NULL) {
            cur_chunk = prev_chunk->next;
            free(prev_chunk);
        }
    }
    free(tree);
}

TreeNode *TreeNode_alloc(Tree *tree) {
    TreeChunk *chunk = tree->tail_chunk;
    if (chunk->offset + sizeof(TreeNode) >= chunk->total_size) {
        TreeChunk *new_chunk = TreeChunk_alloc();

        chunk->next = new_chunk;
        tree->tail_chunk = new_chunk; 
        chunk = new_chunk;

    }

    TreeNode *res = (TreeNode *) (chunk->data + chunk->offset);
    chunk->offset += sizeof(TreeNode);
    tree->n_nodes++;

    return res;
}

int HashPair_compare(HashPair *a, HashPair *b) {
    if (a == NULL) {
        if (b == NULL) {
            return 0;
        } else {
            return -1;
        }
    }
    if (b == NULL) {
        return 1;
    }
    if (a->left == b->left) {
        if (a->right == b->right) {
            return 0;
        } else if (a->right < b->right) {
            return 1;
        } else {
            return -1;
        }
    } else if (a->left < b->left) {
        return 1;
    } else {
        return -1;
    }
}

int TreeNode_hash_compare(TreeNode *node, uint64_t key_left, uint64_t key_right) {
    if (key_left == node->key_left) {
        if (node->key_right == key_right) {
            return 0;
        } else if (node->key_right < key_right) {
            return 1;
        } else {
            return -1;
        }
    } else if (node->key_left < key_left) {
        return 1;
    } else {
        return -1;
    }
}

TreeNode *TreeNode_search(TreeNode *root, uint64_t key_left, uint64_t key_right) {
    TreeNode *current_node = root;
    TreeNode *prev_node = root;
    while(current_node != NULL) {
        int cmp = TreeNode_hash_compare(current_node, key_left, key_right);
        if (cmp == 0) {
            return current_node;
        } else if (cmp < 0) {
            prev_node = current_node;
            current_node = current_node->left;
        } else {
            prev_node = current_node;
            current_node = current_node->right;
        }
    }
    return prev_node;
}

TreeNode *TreeNode_increment(Tree *tree, uint64_t key_left, uint64_t key_right, int inc) {
    TreeNode *root = tree->root;

    if (root == NULL) {
        TreeNode *new_node = TreeNode_alloc(tree);
        tree->root = new_node;
        new_node->key_left = key_left;
        new_node->key_right = key_right;
        new_node->value = inc;
        return new_node;
    } else {

        TreeNode *target = TreeNode_search(root, key_left, key_right);
        int cmp = TreeNode_hash_compare(target, key_left, key_right);
        if (cmp == 0) {
            target->value++;
            return target;
        } else {
            TreeNode *new_node = TreeNode_alloc(tree);
            new_node->value = inc;
            new_node->key_left = key_left;
            new_node->key_right = key_right;
            if (cmp < 0) {
                ASSERT(target->left == NULL, "left is not null!\n")
                target->left = new_node;
            } else {
                ASSERT(target->right == NULL, "right is not null!\n")
                target->right = new_node;
            }
            return new_node;
        }

    }
}

void Tree_write_to_file(Tree *tree, FILE *outf) {

    size_t stack_end_idx = 0;
    TreeNode **stack = (TreeNode **) malloc(sizeof(TreeNode *) * (tree->n_nodes + 1)); /* max possible size */
    
    TreeNode *node = tree->root;

#define STACK_POP (stack_end_idx > 0 ? stack[--stack_end_idx] : NULL)
#define STACK_PUSH(v) stack[stack_end_idx++] = v

    while((stack_end_idx > 0) || (node != NULL)) {

        if (node != NULL) {
            STACK_PUSH(node);
            node = node->left;
        } else {
            node = STACK_POP;

            ASSERT(node != NULL, "fell off left end of traversal stack!!\n")

            uint64_t row[3];
            row[0] = node->key_left;
            row[1] = node->key_right;
            row[2] = node->value;

            fwrite(row, sizeof(uint64_t), 3, outf);

            node = node->right;
        }

    }

    free(stack);

}

void Tree_write_to_buffer(Tree *tree, HashPair *hashes, uint64_t *counts) {

    size_t res_idx = 0;
    
    size_t stack_end_idx = 0;
    TreeNode **stack = (TreeNode **) malloc(sizeof(TreeNode *) * (tree->n_nodes + 1)); /* max possible size */
    
    TreeNode *node = tree->root;

#define STACK_POP (stack_end_idx > 0 ? stack[--stack_end_idx] : NULL)
#define STACK_PUSH(v) stack[stack_end_idx++] = v

    while((stack_end_idx > 0) || (node != NULL)) {

        if (node != NULL) {
            STACK_PUSH(node);
            node = node->left;
        } else {
            node = STACK_POP;

            ASSERT(node != NULL, "fell off left end of traversal stack!!\n")

            HashPair *cur_pair = &hashes[res_idx];
            cur_pair->left = node->key_left;
            cur_pair->right = node->key_right;

            counts[res_idx] = node->value;
            res_idx++;

            if (res_idx % 100000 == 0) {
                sync_printf("%lu\n", res_idx);
            }

            node = node->right;
        }

    }

    free(stack);
}

ssize_t mergesort_merge(
        HashPair **res, uint64_t *res_counts, size_t max_res_size,
        HashPair **inps, uint64_t **inp_counts,
        size_t *inp_sizes, size_t n_slices) {

    if (n_slices < 1) return 0;

    size_t *inp_idxes = malloc(sizeof(size_t) * n_slices);
    memset(inp_idxes, 0, sizeof(size_t) * n_slices);

    printf("%d\n", n_slices);

    uint64_t *min_slices = malloc(sizeof(uint64_t) * n_slices);

    size_t res_idx = 0;
    for (; res_idx < max_res_size; res_idx++) {

        HashPair *current_min = NULL;
        uint64_t current_min_count = 0;

        size_t live_groups_count = 0;
        size_t min_slices_size = 0;

        for (size_t current_slice = 0; current_slice < n_slices; current_slice++) {
            if (inp_idxes[current_slice] >= inp_sizes[current_slice]) continue;
            HashPair *current_k = &(inps[current_slice][inp_idxes[current_slice]]);
            if ((current_min == NULL) || (HashPair_compare(current_k, current_min) < 0)) {
                current_min = current_k;
                min_slices_size = 0;
                min_slices[min_slices_size++] = current_slice;
                current_min_count = inp_counts[current_slice][inp_idxes[current_slice]];
            } else if (current_min->left == current_k->left && current_min->right == current_k->right) {
                min_slices[min_slices_size++] = current_slice;
                current_min_count = inp_counts[current_slice][inp_idxes[current_slice]];
            }
            live_groups_count++;
        }

        if (live_groups_count < 1) break;
        if (current_min == NULL) break;

        for (size_t i=0; i < min_slices_size; i++) {
            inp_idxes[min_slices[i]]++;
        }

        res[res_idx] = current_min;
        res_counts[res_idx] = current_min_count;

    }
    for (size_t slice=0; slice < n_slices; slice++) {
        printf("%lu ", inp_idxes[slice]);
    }
    printf("\n");
    printf("%d\n", res_idx);

    return res_idx;

}

int split_points(size_t n_splits, size_t *splits, size_t *split_sizes, FILE *inf) {

    int infd = fileno(inf);
    struct stat inf_stat;
    if (fstat(infd, &inf_stat) != 0) return -1;
    size_t inf_size = inf_stat.st_size;
    size_t split_size = inf_size / n_splits;
    size_t prev_split = 0;

    for (size_t i=0; i < n_splits; i++) {
        size_t off = split_size * i;
        off -= off % 16; /* shift to row boundary */
        if (fseek(inf, off, SEEK_SET) != 0) return -1;

        uint64_t cur_row[2];
        uint64_t prev_hash = 0;
        while(1) {
            if (fread(&cur_row, sizeof(uint64_t), 2, inf) < 0) return -1;
            uint64_t cur_hash = cur_row[0];

            if (prev_hash == 0) {
                prev_hash = cur_hash;
            } else if (prev_hash != cur_hash) {
                break;
            }
            off += 16;

        }

        splits[i] = off;
        if (i > 0) {
            split_sizes[i - 1] = off;
        }
        prev_split = off;

    }

    split_sizes[n_splits - 1] = inf_size;

    return 0;

}

typedef struct WorkerArgs {

    void *inf_buf;
    size_t split_offset;
    size_t split_end;
    HashPair *res_hashes;
    uint64_t *res_counts;
    size_t res_size;

} WorkerArgs;

void *worker(void *_args) {

    WorkerArgs *args = (WorkerArgs *) _args;
    uint64_t *inf_buf = args->inf_buf;
    size_t row_idx = args->split_offset / 16;
    size_t row_start = row_idx;
    size_t max_row_idx = args->split_end / 16;
    
    Tree *tree = Tree_alloc();
    
    uint64_t current_key_hash;
    uint64_t prev_key_hash = 0;
    uint64_t current_val_hash;
    uint64_t *current_val_hashes = malloc(VAL_GROUP_SIZE * sizeof(uint64_t));
    size_t current_val_hashes_idx = 0;

    for (; row_idx < max_row_idx; row_idx++)  {
        current_key_hash = inf_buf[row_idx * 2];
        current_val_hash = inf_buf[row_idx * 2 + 1];

        if (row_idx % 100000 == 0) {
            sync_printf("%lu %lu\n", row_idx - row_start, tree->n_nodes);
        }

        if (current_key_hash == prev_key_hash && current_val_hashes_idx < VAL_GROUP_SIZE) {
            current_val_hashes[current_val_hashes_idx++] = current_val_hash;

        } else {

            size_t idx_mid = current_val_hashes_idx / 2;
            for (size_t idx_left=idx_mid; idx_left > 0; idx_left--) {
                for (size_t idx_right=current_val_hashes_idx; idx_right > idx_mid; idx_right--) {
                    uint64_t v_left = current_val_hashes[idx_left];
                    uint64_t v_right = current_val_hashes[idx_right];

                    if (v_left == v_right) continue;

                    TreeNode_increment(tree, v_left, v_right, 1);
                    TreeNode_increment(tree, v_right, v_left, 1);

                }

            }

            current_val_hashes[0] = current_val_hash;
            current_val_hashes_idx = 1;
            prev_key_hash = current_key_hash;

        }

    }

    size_t n_rows = tree->n_nodes;

    args->res_size = n_rows;

    sync_printf("tree done\n");

    HashPair *res_hashes = malloc(sizeof(HashPair) * n_rows);
    uint64_t *res_counts = malloc(sizeof(uint64_t) * n_rows);

    Tree_write_to_buffer(tree, res_hashes, res_counts);
    /*Tree_free(tree);*/

    args->res_hashes = res_hashes;
    args->res_counts = res_counts;

    sync_printf("thread done\n");

    pthread_exit(NULL);

}

int main(int argc, char **argv) {

    if (argc != 4) {
        printf("usage: %s <inp.bin> <outp.bin> <n_cores>\n", argv[0]);
        return 1;
    }

    pthread_mutex_init(&printf_mutex, NULL);

    FILE *outf = fopen(argv[2], "w");

    char *infname = argv[1];
    FILE *inf = fopen(infname, "r");

    size_t n_cores = atol(argv[3]);

    size_t *splits = malloc(sizeof(size_t) * n_cores);
    size_t *split_sizes = malloc(sizeof(size_t) * n_cores);
    split_points(n_cores, splits, split_sizes, inf);

    fclose(inf);

    void *inf_buf = mmap_read_file(infname);

    if (inf_buf == 0) return -1;

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

    WorkerArgs *worker_args = malloc(sizeof(WorkerArgs) * n_cores);
    pthread_t *threads = malloc(sizeof(pthread_t) * n_cores);

    for (size_t core_id=0; core_id < n_cores; core_id++) {
        WorkerArgs *args = &worker_args[core_id];
        args->inf_buf = inf_buf;
        args->split_offset = splits[core_id];
        args->split_end = split_sizes[core_id];
        args->res_hashes = NULL;
        args->res_counts = NULL;

        pthread_create(&threads[core_id], &attr, worker, (void *) args);

    }

    void *status;
    pthread_attr_destroy(&attr);
    for (size_t core_id=0; core_id < n_cores; core_id++) {
        pthread_join(threads[core_id], &status);
    }

    size_t max_size = 0;
    for (size_t i=0; i < n_cores; i++) {
        max_size += worker_args[i].res_size;
    }

    HashPair **res = malloc(sizeof(HashPair *) * max_size);
    uint64_t *res_counts = malloc(sizeof(uint64_t) * max_size);

    HashPair **inps = malloc(sizeof(HashPair *) * n_cores);
    uint64_t **inp_counts = malloc(sizeof(uint64_t *) * n_cores);
    size_t *inp_sizes = malloc(sizeof(size_t) * n_cores);
    for (size_t i=0; i < n_cores; i++) {
        inps[i] = worker_args[i].res_hashes;
        inp_counts[i] = worker_args[i].res_counts;
        inp_sizes[i] = worker_args[i].res_size;
    }

    ssize_t merged_size = mergesort_merge(
            res, res_counts, max_size,
            inps, inp_counts,
            inp_sizes, n_cores);

    for (size_t i=0; i < merged_size; i++) {
        uint64_t row[3];
        HashPair *p = res[i];
        row[0] = p->left;
        row[1] = p->right;
        row[2] = res_counts[i];

        fwrite(&row, sizeof(row), 1, outf);
    }

    fclose(outf);

}
