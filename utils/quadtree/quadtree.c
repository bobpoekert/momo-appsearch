#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <omp.h>
#include <alloca.h>
#include <string.h>
#include <stdio.h>
#include "quadtree.h"

/*

    1. generate full-resolution hashes
    2. find longest number of bits for which 
       the longest common prefix with that many bits is shorter than the block size
    3. generate list of prefix counts with prefixes of that length
    4. repeat 2 and 3 with output of 3 until the result has only one block

    metadata:
        - min x
        - max x
        - min y
        - max y
        - hash levels

*/


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


quadtree__tree *quadtree__init(size_t n_points, uint32_t *points) {
    quadtree__tree *res = malloc(sizeof(quadtree__tree));
    res->n_points = n_points;
    size_t row_size = sizeof(quadtree__z_value);
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
    
    uint64_t pivot = v[a[len / 2]];
    
    int i, j;
    for (i = 0, j = len - 1; ; i++, j--) {
        while (v[a[i]] < pivot) i++;
        while (v[a[j]] > pivot) j--;
    
        if (i >= j) break;
    
        uint32_t temp = a[i];
        a[i]     = a[j];
        a[j]     = temp;
    }
    
    quicksort(a, v, i);
    quicksort(a + i, v, len - i);
}

void radix_sort(uint32_t *a, uint64_t *v, size_t len) {
    if (len < 2) return;

    uint64_t max = 0;
    for (size_t i=0; i < len; i++) {
        uint64_t vv = v[i];
        if (vv > max) max = vv;
    }

    int buckets[10];
    uint32_t *scratch = malloc(len * sizeof(uint32_t));

    for (size_t x=1; max / x > 0; x *= 10) {
        memset(buckets, 0, sizeof(int) * 10);
        for (size_t k=0; k < len; k++) {
            uint64_t vv = v[a[k]];
            buckets[(vv / x) % 10]++;
        }

        for (size_t l=1; l < 10; l++) {
            /* turn counts into offsets */
            buckets[l] += buckets[l-1];
        }

        for (size_t j=len-1; j > 0; j--) {
            uint64_t vv = v[a[j]];
            size_t bucket_idx = (vv / x) % 10;
            scratch[buckets[bucket_idx] - 1] = a[j];
            buckets[bucket_idx]--;
        }

        memcpy(a, scratch, len * sizeof(uint32_t));
    }

    free(scratch);
}


void merge_partitions(uint32_t **partitions, uint64_t *v, uint32_t *res,
                      size_t n_partitions, size_t n_rows) {
    size_t res_idx = 0;
    size_t partition_idxes_size = n_partitions * sizeof(size_t);
    size_t *partition_idxes = alloca(partition_idxes_size);
    memset(partition_idxes, 0, partition_idxes_size);
    size_t partition_size = n_rows / n_partitions;
    while (res_idx < n_rows) {
        uint32_t pidx = partitions[0][partition_idxes[0]];
        uint64_t min_val = v[pidx];
        uint32_t min_val_idx = pidx;
        size_t min_partition;
        for (size_t partition=1; partition < n_partitions; partition++) {
            if (partition_idxes[partition] > partition_size) continue;
            pidx = partitions[partition][partition_idxes[partition]];
            uint64_t cv = v[pidx];
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
            radix_sort(partition, self->rows, partition_size);
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