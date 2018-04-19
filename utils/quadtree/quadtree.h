#include <unistd.h>

typedef uint64_t quadtree__z_value;

/* x and y are non-negative integers */

typedef struct quadtree__tree {
    uint64_t n_points;
    uint64_t hash_size;
    uint64_t tree_depth;
    uint64_t max_x;
    uint64_t max_y;
    uint64_t min_x;
    uint64_t min_y;
    quadtree__z_value *rows;
} quadtree__tree;

typedef char *quadtree__hash_t;