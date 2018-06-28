#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

#include "util.h"

#define MIN(a,b) (a > b ? b : a)
#define MAX(a,b) (a > b ? a : b)

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#define ASSERT(v, msg) if (unlikely(!(v))) { printf(msg); abort(); }

typedef struct TreeNode {

    uint64_t key_left;
    uint64_t key_right;
    uint64_t value;
    struct TreeNode *left;
    struct TreeNode *right;

} TreeNode;

#define TREE_CHUNK_SIZE 409600

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

int main(int argc, char **argv) {

    if (argc != 2) {
        printf("usage: %s <outp.bin> < inp\n", argv[0]);
        return 1;
    }

    Tree *tree = Tree_alloc();
    FILE *outf = fopen(argv[1], "w");


    size_t row_idx = 0;

    uint64_t current_key_hash;
    uint64_t prev_key_hash = 0;
    uint64_t current_val_hash;
    uint64_t *current_val_hashes = malloc(TREE_CHUNK_SIZE * sizeof(uint64_t));
    size_t current_val_hashes_idx = 0;

    char *current_line = (char *) malloc(1024);
    size_t line_buf_size = 1024;
    ssize_t current_line_size;

    size_t current_key_split_point;

    while(1) {
        current_line_size = getline(&current_line, &line_buf_size, stdin);
        if (current_line_size < 0) {
            if (feof(stdin)) {
                break;
            } else {
                continue;
            }
        }

        current_line_size--; /* strip trailing newline */

        row_idx++;
        if (row_idx % 10000 == 0) {
            printf("%lu %lu\n", row_idx, tree->n_nodes);
        }

        current_key_split_point = tab_col_split_point(current_line, current_line_size, 2);
        current_key_hash = hash_bytes(current_line, current_key_split_point);
        current_val_hash = hash_bytes(
                current_line + current_key_split_point,
                current_line_size - current_key_split_point);

        if (current_key_hash == prev_key_hash && current_val_hashes_idx < TREE_CHUNK_SIZE) {
            current_val_hashes[current_val_hashes_idx++] = current_val_hash;

        } else {

            size_t idx_mid = current_val_hashes_idx / 2;
            for (size_t idx_left=idx_mid; idx_left > 0; idx_left--) {
                for (size_t idx_right=current_val_hashes_idx; idx_right > idx_mid; idx_right--) {
                    uint64_t v_left = current_val_hashes[idx_left];
                    uint64_t v_right = current_val_hashes[idx_right];

                    if (v_left == v_right) continue;

                    TreeNode_increment(tree, MIN(v_left, v_right), MAX(v_left, v_right), 1);

                }

            }

            current_val_hashes[0] = current_val_hash;
            current_val_hashes_idx = 1;
            prev_key_hash = current_key_hash;

        }

    }

    Tree_write_to_file(tree, outf);
    fclose(outf);

}
