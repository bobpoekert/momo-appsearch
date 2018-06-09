#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include "murmur3.c"

#define TREE_CHUNK_SIZE 1000
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

TreeChunk *tree_chunk_alloc() {
    TreeChunk *res = malloc(sizeof(TreeChunk));
    memset(res->payload, 0, sizeof(TreeNode));
    res->size = 0;
    res->next = 0;
    return res;
}

Tree *tree_alloc() {
    Tree *res = malloc(sizeof(Tree));
    TreeChunk *chunk = tree_chunk_alloc();
    res->head_chunk = chunk;
    res->tail_chunk = chunk;
    res->size = 0;
    return res;
}

TreeNode *tree_node_alloc(Tree *tree) {
    if (tree->tail_chunk == NULL || tree->tail_chunk->size >= TREE_CHUNK_SIZE) {
        TreeChunk *new_chunk = malloc(sizeof(TreeChunk));
        new_chunk->next = 0;
        new_chunk->size = 0;
        tree->tail_chunk->next = new_chunk;
        tree->tail_chunk = new_chunk;
    }
    if (tree->head_chunk == NULL) {
        tree->head_chunk = tree->tail_chunk;
    }
    tree->tail_chunk->size++;
    TreeNode *res = &(tree->tail_chunk->payload[tree->tail_chunk->size]);
    res->left = 0;
    res->right = 0;
    res->hash = 0;
    res->value = 0;
    res->count = 0;
    tree->size++;
    return res;
}

TreeNode *tree_node_alloc_kv(Tree *tree, Hash a, Hash b) {
    TreeNode *new_node = tree_node_alloc(tree);
    new_node->hash = a;
    TreeNode *new_b = tree_node_alloc(tree);
    new_b->hash = b;
    new_b->count = 1;
    new_node->value = new_b;
    return new_node;
}

void tree_insert_hash_pair(Tree *tree, Hash a, Hash b) {
    printf("\n\n");
    TreeNode *current_node = tree->head_chunk->payload;
    TreeNode *value_node = 0;
    printf("%x %x\n", a, b);
    if (a == 0) printf("a is zero!\n");
    if (b == 0) printf("b is zero!\n");
    if (current_node->hash == 0) {
        current_node->hash = a;
        current_node->count = 0;
        current_node->value = tree_node_alloc(tree);
        current_node->value->hash = b;
        current_node->value->count = 1;
        return;
    }
    while(1) {
        printf("%x\n", current_node->hash);
        if (current_node->hash == a) {
            if (current_node->value == NULL) {
                current_node->value = tree_node_alloc(tree);
                current_node->value->hash = b;
                current_node->value->count = 1;
                return;
            } else {
                value_node = current_node->value;
                break;
            }
        } else if (current_node->hash < a) {
            if (current_node->left == NULL) {
                current_node->left = tree_node_alloc_kv(tree, a, b);
                current_node = current_node->left->value;
                return;
            } else {
                current_node = current_node->left;
            }
        } else {
            if (current_node->right == NULL) {
                current_node->right = tree_node_alloc_kv(tree, a, b);
                current_node = current_node->right->value;
                return;
            } else {
                current_node = current_node->right;
            }
        }
    }
    printf("--\n");
    while(1) {
        printf("%x\n", value_node->hash);
        if (value_node->hash == b) {
            value_node->count++;
            break;
        } else if (value_node->hash < b) {
            if (value_node->left == NULL) {
                TreeNode *new_node = tree_node_alloc(tree);
                new_node->hash = b;
                new_node->count = 1;
                value_node->left = new_node;
                break;
            } else {
                value_node = value_node->left;
            }
        } else {
            TreeNode *new_node = tree_node_alloc(tree);
            new_node->hash = b;
            if (value_node->right == NULL) {
                new_node->count = 1;
                value_node->right = new_node;
                break;
            } else {
                value_node = new_node;
            }
        }
    }
}



void write_int(int fd, uint32_t *buf, size_t *idx, uint32_t v) {
    if (*idx >= BUFFER_SIZE) {
        write(fd, buf, *idx);
        *idx = 0;
    }
    buf[*idx] = v;
    *idx++;
}

void tree_write_file(Tree *tree, int fd) {
    uint32_t *output_buffer = malloc(BUFFER_SIZE);
    size_t idx = 0;
    /*size_t stack_size = (log10((double) tree->size) / 0.301029995663981 ) * 2;*/
    size_t stack_size = 100000;
    TreeNode **stack = malloc(sizeof(TreeNode *) * stack_size);
    memset(stack, 0, sizeof(TreeNode *) * stack_size);
    int stack_idx = 0;

    TreeNode *cur = tree->head_chunk->payload;
    Hash k = 0;

#define WRITE_INT(v) \
    if (idx >= BUFFER_SIZE) {\
        write(fd, output_buffer, idx);\
        idx = 0;\
    }\
    output_buffer[idx] = v;\
    idx++;

    do {
        if (cur != NULL) {
            if (cur->left != 0)
                printf("\"%x\" -> \"%x\" [color=red];\n", cur->hash, cur->left->hash);
            if (cur->value != 0)
                printf("\"%x\" -> \"%x\" [color=green];\n", cur->hash, cur->value->hash);
            if (cur->right != 0)
                printf("\"%x\" -> \"%x\" [color=blue];\n", cur->hash, cur->right->hash);
        }
        if (cur == NULL) {
            stack_idx--;
            cur = stack[stack_idx];
            if (cur->value != 0) {
                k = 0;
            } else {
                cur = cur->right;
            }
        } else if (cur->value == 0) {
            if (k != 0) {
                WRITE_INT(k)
                WRITE_INT(cur->hash)
                WRITE_INT(cur->count)
            }
            stack[stack_idx] = cur;
            stack_idx++;
            cur = cur->left;
        } else {
            k = cur->hash;
            cur = cur->value;
        }
    } while(stack_idx > 0);

    if (idx > 0) {
        write(fd, output_buffer, idx);
    }

    free(output_buffer);
    free(stack);
}

int main(int argc, char **argv) {

    char *infname = argv[1];
    int infd = open(infname, O_RDONLY);

    char *outfname = argv[2];
    int outfd = open(outfname, O_CREAT | O_RDWR, S_IRWXU);

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

    size_t current_buffer_size = 0;

    /*
     * 0: reading app name
     * 1: column 1 (skip)
     * 2: reading value
     */
    char state = 0;

    Tree *tree = tree_alloc();

    current_buffer_size = read(infd, input_buffer, BUFFER_SIZE);
    while(current_buffer_size > 1) {
        if (input_buffer_idx >= current_buffer_size) {
            current_buffer_size = read(infd, input_buffer, BUFFER_SIZE);
            if (current_buffer_size == EOF) {
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

    tree_write_file(tree, outfd);

}
