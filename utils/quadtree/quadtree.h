#include <unistd.h>

typedef uint64_t quadtree__z_value;

/* x and y are non-negative integers */

typedef struct quadtree__tree {
    uint64_t n_points;
    uint64_t max_x;
    uint64_t max_y;
    uint64_t min_x;
    uint64_t min_y;
    quadtree__z_value *rows;
} quadtree__tree;

typedef char *quadtree__hash_t;

quadtree__tree *quadtree__init(size_t n_points, uint32_t *points);
void quadtree__free(quadtree__tree *);
void quadtree__sort(quadtree__tree *self, uint32_t *indexes);