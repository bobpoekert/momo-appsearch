#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define BIG_CONSTANT(x) (x##LLU)
#define SEED 0xdeadbeef

//-----------------------------------------------------------------------------
// MurmurHash2, 64-bit versions, by Austin Appleby

// The same caveats as 32-bit MurmurHash2 apply here - beware of alignment 
// and endian-ness issues if used across multiple platforms.

// 64-bit hash for 64-bit platforms

uint64_t hash_bytes( const void * key, size_t len) {
    const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
    const int r = 47;

    uint64_t h = SEED ^ (len * m);

    const uint64_t * data = (const uint64_t *)key;
    const uint64_t * end = data + (len/8);

    while(data != end) {
        uint64_t k = *data++;

        k *= m; 
        k ^= k >> r; 
        k *= m; 

        h ^= k;
        h *= m; 
    }

    const unsigned char * data2 = (const unsigned char*)data;

    switch(len & 7) {
        case 7: h ^= ((uint64_t) data2[6]) << 48;
        case 6: h ^= ((uint64_t) data2[5]) << 40;
        case 5: h ^= ((uint64_t) data2[4]) << 32;
        case 4: h ^= ((uint64_t) data2[3]) << 24;
        case 3: h ^= ((uint64_t) data2[2]) << 16;
        case 2: h ^= ((uint64_t) data2[1]) << 8;
        case 1: h ^= ((uint64_t) data2[0]);
                h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
} 



#define HEAP_ROW_SIZE (sizeof(uint64_t) * 2)
uint64_t heap_insert_counts_uint32(
        uint64_t *heap, size_t n_heap_items, size_t max_heap_items,
        uint64_t new_item) {

    size_t heap_start = 0;
    size_t heap_end = n_heap_items;

    while(heap_end > heap_start) {
        size_t heap_mid = heap_start + (heap_end - heap_start) / 2;
        size_t heap_mid_idx = heap_mid * 2;
        uint64_t target = heap[heap_mid_idx];
        if (target == new_item) {
            heap[heap_mid_idx + 1]++;
            return heap[heap_mid_idx + 1];
        } else if (target > new_item) {
            /* to the right */
            heap_start = heap_mid;
        } else {
            /* to the left */
            heap_end = heap_mid;
        }
    }

    /* item not found */

    if (n_heap_items < max_heap_items) {
        /* heap isn't full, move everything after insertion point to the right to make room */
        memmove(
                heap + (heap_start + 2) * HEAP_ROW_SIZE, heap + heap_start * HEAP_ROW_SIZE,
                n_heap_items * HEAP_ROW_SIZE - heap_start * HEAP_ROW_SIZE);
        heap[heap_start] = new_item;
        heap[heap_start + 1] = 1;
    } else {

        uint64_t min_count = 0xffffffffffffffff;
        uint64_t min_count_idx = 0;
        for (size_t i=1; i < n_heap_items; i += 2) {
            uint64_t current_count = heap[i];
            if (current_count < min_count) {
                min_count = current_count;
                min_count_idx = i-1;
            }
        }

        if (min_count_idx < heap_start) {
            /* shift to the left */
            /* take range between min_count_idx and heap start 
             * shift left by one to make room for new item
             * insert new item at heap_start
             */

            /* destination = min_count_idx
             * source = one to the right of min_count_idx
             * length = range between min_count_idx and heap_start
             */
            memmove(heap + min_count_idx * HEAP_ROW_SIZE,
                    heap + (min_count_idx + 1) * HEAP_ROW_SIZE,
                    (heap_start - min_count_idx) * HEAP_ROW_SIZE);

            heap[heap_start] = new_item;
            heap[heap_start + 1] = 1;

        } else {
            /* shift to the right */

            /* destination = min_count_idx
             * source = one to the left of min_count_idx
             * length = range between heap_start and min_count_idx
             */

            memmove(heap + min_count_idx * HEAP_ROW_SIZE,
                    heap + (min_count_idx - 1) * HEAP_ROW_SIZE,
                    (min_count_idx - heap_start) * HEAP_ROW_SIZE);

            heap[heap_start] = new_item;
            heap[heap_start + 1] = 1;
        }


    }
    return 0;

}

#define CACHE_HEAP_SIZE 4000

void hashes_from_fd(int inp_fd, char *hashes_fname, char *strings_fname) {

    size_t buffer_size;
    char *current_line;
    ssize_t line_size;
    uint64_t outp_line_size;

    uint64_t current_hash;
    uint64_t current_strings_offset;

    size_t heap_size;
    uint64_t *heap;
    uint64_t heap_insert_res;

    FILE *hashes_f;
    FILE *strings_f;
    FILE *inp_f;

    inp_f = fdopen(inp_fd, "r");
    hashes_f = fopen(hashes_fname, "w");
    strings_f = fopen(strings_fname, "w");

    buffer_size = 1024 * 1024;
    current_line = malloc(buffer_size);

    current_strings_offset = 0;

    heap = malloc(CACHE_HEAP_SIZE * HEAP_ROW_SIZE);
    heap_size = 0;

    size_t max_line_size = 0;

    while(1) {
        line_size = getline(&current_line, &buffer_size, inp_f);
        if (line_size < 0) break;
        if (line_size < 1) continue;
        if (line_size > 100000) continue;
        line_size--; /* strip trailing newline */

        current_hash = hash_bytes(current_line, line_size);

        if (line_size > max_line_size) max_line_size = line_size;

        /* clip off the most frequent duplicates using a top-k heap
         * heap size is chosen to be small enough to fit in L2 cache
         * this should be a significant perf improvement if the
         * frequency distribution is zipfian-ish
         */
        heap_insert_res = heap_insert_counts_uint32(
                heap, heap_size, CACHE_HEAP_SIZE,
                current_hash);

        if (heap_insert_res < 1) {


            if (fwrite(&current_hash, sizeof(current_hash), 1, hashes_f) < 1) break;
            if (fwrite(&current_strings_offset, sizeof(current_strings_offset), 1, hashes_f) < 1) break;
            
            outp_line_size = line_size;
            if (fwrite(&outp_line_size, sizeof(outp_line_size), 1, strings_f) < 1) break;
            current_strings_offset += sizeof(outp_line_size);
            if (fwrite(current_line, line_size, 1, strings_f) < 1) break;
            current_strings_offset += line_size;

        }

    }

    printf("max line size: %d\n", max_line_size);
    fclose(hashes_f);
    fclose(strings_f);
    free(current_line);

}
