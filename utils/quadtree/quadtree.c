#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <omp.h>
#include <alloca.h>
#include "quadtree.h"

#define NW 0
#define NE 1
#define SW 2
#define SE 3

#define LOG4(t) (log10(t) / 0.602059991327962)

quadtree__z_value quadtree__get_z_value(uint32_t x, uint32_t y) {
    x = (x | (x << 16)) & 0x0000FFFF0000FFFF;
    x = (x | (x << 8)) & 0x00FF00FF00FF00FF;
    x = (x | (x << 4)) & 0x0F0F0F0F0F0F0F0F;
    x = (x | (x << 2)) & 0x3333333333333333;
    x = (x | (x << 1)) & 0x5555555555555555;

    y = (y | (y << 16)) & 0x0000FFFF0000FFFF;
    y = (y | (y << 8)) & 0x00FF00FF00FF00FF;
    y = (y | (y << 4)) & 0x0F0F0F0F0F0F0F0F;
    y = (y | (y << 2)) & 0x3333333333333333;
    y = (y | (y << 1)) & 0x5555555555555555;

    return x | (y << 1);
}

/* size in bits of the hash value for a tree with given number of points */
size_t quadtree__hash_size(uint64_t n_points) {
    return LOG4(n_points) * 2 / 4;
}

size_t quadtree__depth(uint64_t n_points) {
    return n_points * LOG4(n_points);
}


quadtree__tree *quadtree__init(size_t n_points, uint32_t *points) {
    quadtree__tree *res = malloc(sizeof(quadtree__tree));
    res->n_points = n_points;
    res->hash_size = quadtree__hash_size(n_points);
    res->tree_depth = quadtree__depth(n_points);
    size_t row_size = res->hash_size;
    res->rows = malloc(n_points * row_size);
    uint64_t max_x = 0;
    uint64_t max_y = 0;
    uint64_t min_x = -1;
    uint64_t min_y = -1;
    for (size_t i=0; i < n_points; i += 2) {
        uint64_t x = points[i];
        if (x > max_x) max_x = x;
        if (x < min_x) min_x = x;
        
        uint64_t y = points[i + 1];
        if (y > max_y) max_y = y;
        if (y < min_y) min_y = y;
    }
    res->max_x = max_x;
    res->max_y = max_y;
    res->min_x = min_x;
    res->min_y = min_y;
    for (size_t i=0; i < n_points; i += 2) {
        uint64_t x = points[i];
        uint64_t y = points[i+1];
        uint32_t scaled_x = (x - min_x);
        uint32_t scaled_y = y - min_y;
        res->rows[i/2] = quadtree__get_z_value(scaled_x, scaled_y);
    }
    return res;
}

void quadtree__free(quadtree__tree *v) {
    free(v->rows);
    free(v);
}

void quicksort(uint32_t *a, uint64_t *v, size_t len) {
    if (len < 2) return;
    
    int pivot = v[a[len / 2]];
    
    int i, j;
    for (i = 0, j = len - 1; ; i++, j--) {
        while (v[a[i]] < pivot) i++;
        while (v[a[j]] > pivot) j--;
    
        if (i >= j) break;
    
        int temp = a[i];
        a[i]     = a[j];
        a[j]     = temp;
    }
    
    quicksort(a, v, i);
    quicksort(a + i, v, len - i);
}

void merge_partitions(uint32_t **partitions, uint64_t *v, uint32_t *res,
                      size_t n_partitions, size_t n_rows) {
    size_t res_idx = 0;
    size_t *partition_idxes = alloca(n_partitions * sizeof(size_t));
    size_t partition_size = n_rows / n_partitions;
    while (res_idx < n_rows) {
        uint64_t min_val = -1;
        uint32_t min_val_idx;
        size_t min_partition;
        for (size_t partition=0; partition < n_partitions; partition++) {
            uint32_t pidx = partition_idxes[partition];
            if (pidx >= partition_size) continue;
            uint64_t cv = v[partitions[partition][pidx]];
            if (cv < min_val) {
                min_val = cv;
                min_val_idx = pidx;
                min_partition = partition;
            }
        }
        res[res_idx] = min_val_idx;
        partition_idxes[min_partition]++;
        res_idx++;
    }
}

void quadtree__sort(
    quadtree__tree *self,
    uint32_t *indexes) {

    size_t n_threads;
    size_t partition_size;
    uint32_t **partitions;

    #pragma omp parallel
    {

        #pragma omp single
        {
            n_threads = omp_get_num_threads();
            partition_size = self->n_points / n_threads;
            partitions = malloc(sizeof(uint32_t *) * n_threads);
        }

        #pragma omp for schedule(static) ordered
        for (size_t thread_id=0; thread_id < n_threads; thread_id++) {
            uint32_t *partition =  malloc(sizeof(uint32_t) * partition_size);
            size_t partition_start = thread_id * partition_size;
            for (size_t ctr=0; ctr < partition_size; ctr++) {
                partition[ctr] = ctr + partition_start;
            }
            quicksort(partition, self->rows, partition_size);
            partitions[thread_id] = partition;
        }

        #pragma omp single
        {
            merge_partitions(partitions, self->rows, indexes, n_threads, self->n_points);
            for (size_t partition=0; partition < n_threads; partition++) {
                free(partitions[partition]);
            }
            free(partitions);
        }

    }
}