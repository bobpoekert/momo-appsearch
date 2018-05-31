#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include "murmur3.c"

#define TREE_CHUNK_SIZE 100000
#define BUFFER_SIZE 16777216
#define SEED 0xdeadbeef

typedef uint32_t Hash;

typedef struct TreeNode {
    struct TreeNode *left;
    struct TreeNode *right;
    Hash hash;
    struct TreeNode *value;
    uint64_t count;
} TreeNode;

typedef struct TreeChunk {
    size_t size;
    struct TreeChunk *next;
    TreeNode payload[TREE_CHUNK_SIZE];
} TreeChunk;

typedef struct Tree {
    TreeChunk *head_chunk;
    TreeChunk *tail_chunk;
    size_t size;
} Tree;

Tree *tree_alloc() {
    Tree *res = malloc(sizeof(Tree));
    res->head_chunk = 0;
    res->tail_chunk = 0;
    res->size = 0;
    return res;
}

TreeNode *tree_node_alloc(Tree *tree) {
    if (tree->tail_chunk == NULL || tree->tail_chunk->size > TREE_CHUNK_SIZE) {
        TreeChunk *new_chunk = malloc(sizeof(TreeChunk));
        new_chunk->next = 0;
        new_chunk->size = 0;
        tree->tail_chunk->next = new_chunk;
        tree->tail_chunk = new_chunk;
    }
    if (tree->head_chunk == NULL) {
        tree->head_chunk = tree->tail_chunk;
    }
    TreeNode *res = &(tree->tail_chunk->payload[tree->tail_chunk->size]);
    memset(res, 0, sizeof(TreeNode));
    tree->tail_chunk->size++;
    tree->size++;
    return res;
}

TreeNode *tree_node_alloc_kv(Tree *tree, Hash a, Hash b) {
    TreeNode *new_node = tree_node_alloc(tree);
    new_node->hash = a;
    TreeNode *new_b = tree_node_alloc(tree);
    new_b->hash = b;
    new_node->value = new_b;
    new_node->count = -1;
    new_b->count = 1;
    return new_node;
}

void tree_insert_hash_pair(Tree *tree, Hash a, Hash b) {
    TreeNode *current_node = tree->head_chunk->payload;
    while(1) {
        if (current_node->hash == a) {
            if (current_node->value == NULL) {
                current_node->value = tree_node_alloc(tree);
                current_node->value->hash = b;
                current_node->value->count = 1;
                return;
            } else {
                current_node = current_node->value;
            }
            break;
        } else if (current_node->hash < a) {
            if (current_node->left == NULL) {
                current_node->left = tree_node_alloc_kv(tree, a, b);
                return;
            } else {
                current_node = current_node->left;
            }
        } else {
            if (current_node->right == NULL) {
                current_node->right = tree_node_alloc_kv(tree, a, b);
                return;
            } else {
                current_node = current_node->right;
            }
        }
    }
    while(1) {
        if (current_node->hash == b) {
            current_node->count++;
            break;
        } else if (current_node->hash < b) {
            if (current_node->left == NULL) {
                TreeNode *new_node = tree_node_alloc(tree);
                new_node->count = 1;
                current_node->left = new_node;
                break;
            } else {
                current_node = current_node->left;
            }
        } else {
            TreeNode *new_node = tree_node_alloc(tree);
            if (current_node->right == NULL) {
                new_node->count = 1;
                current_node->right = new_node;
                break;
            } else {
                current_node = new_node;
            }
        }
    }
}



inline void write_int(int fd, uint32_t *buf, size_t *idx, uint32_t v) {
    if (*idx >= BUFFER_SIZE) {
        write(fd, buf, *idx);
        *idx = 0;
    }
    buf[*idx] = v;
    *idx++;
}

void tree_write_file(Tree *tree, int fd) {
    uint32_t output_buffer[BUFFER_SIZE];
    size_t idx = 0;
    size_t stack_size = (log10((double) tree->size) / 0.301029995663981 /* log10(2) */) * 2;
    TreeNode **stack = malloc(sizeof(TreeNode *) * stack_size);
    memset(stack, 0, sizeof(TreeNode *) * stack_size);
    int stack_idx = 0;

    TreeNode *cur = tree->head_chunk->payload;
    Hash k;

    do {
        if (cur == NULL) {
            stack_idx--;
            cur = stack[stack_idx];
            if (cur->count == -1) {
                k = cur->hash;
                cur = cur->value;
            } else {
                write_int(fd, output_buffer, &idx, k);
                write_int(fd, output_buffer, &idx, cur->hash);
                write_int(fd, output_buffer, &idx, cur->count);
                cur = cur->right;
            }
        } else {
            stack[stack_idx] = cur;
            stack_idx++;
            cur = cur->left;
        }
    } while(stack_idx > 0);
}

int main(int argc, char **argv) {

    char current_app_name[4096];
    size_t current_app_name_size = 0;

    char reading_app_name[4096];
    size_t reading_app_name_size = 0;

    Hash *current_app_hashes = malloc(sizeof(Hash) * 10000);
    size_t app_hashes_idx = 0;
    char *input_buffer = malloc(BUFFER_SIZE);
    size_t input_buffer_idx = 0;

    char *current_line = malloc(BUFFER_SIZE);
    size_t current_line_idx = 0;

    /*
     * 0: reading app name
     * 1: column 1 (skip)
     * 2: reading value
     */
    char state = 0;

    Tree *tree = tree_alloc();

    if (read(0, input_buffer, BUFFER_SIZE) == EOF) return 1;
    while(1) {
        if (input_buffer_idx >= BUFFER_SIZE) {
            if (read(0, input_buffer, BUFFER_SIZE) == EOF) {
                for (size_t l=0; l < app_hashes_idx; l++) {
                    for (size_t r=0; r < app_hashes_idx; r++) {
                        if (l != r) {
                            tree_insert_hash_pair(tree, current_app_hashes[l], current_app_hashes[r]);
                        }
                    }
                }
                break;
            }
            input_buffer_idx = 0;
        }
        char c = input_buffer[input_buffer_idx];
        switch(state) {
            case 0:
                if (c == '\t') {
                    if (current_app_name_size != reading_app_name_size ||
                            strncmp(current_app_name, reading_app_name, reading_app_name_size) != 0) {

                        for (size_t l=0; l < app_hashes_idx; l++) {
                            for (size_t r=0; r < app_hashes_idx; r++) {
                                if (l != r) {
                                    tree_insert_hash_pair(tree, current_app_hashes[l], current_app_hashes[r]);
                                }
                            }
                        }

                        memcpy(current_app_name, reading_app_name, reading_app_name_size);
                        current_app_name_size = reading_app_name_size;
                        reading_app_name_size = 0;
                    }
                    state = 1;
                } else {
                    reading_app_name[reading_app_name_size] = c;
                    reading_app_name_size++;
                }
                break;
            case 1:
                if (c == '\t') {
                    state = 2;
                }
                break;
            case 2:
                if (c == '\n') {
                    MurmurHash3_x86_32(current_line, current_line_idx,
                            SEED, &(current_app_hashes[app_hashes_idx]));
                    app_hashes_idx++;
                    current_line_idx = 0;
                    state = 0;
                } else {
                    current_line[current_line_idx] = c;
                    current_line_idx++;
                }
                break;
        }
        input_buffer_idx++;
    }

    tree_write_file(tree, 1);

}
