#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* transliterated for comaptibility from: https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/Murmur3.java */

#define SEED 0
#define C1 0xcc9e2d51
#define C2 0x1b873593

inline uint32_t rotl32 ( uint32_t x, int8_t r ) {
  return (x << r) | (x >> (32 - r));
}

inline uint32_t mixK1(uint32_t k1) {
    k1 *= C1;
    k1 = rotl32(k1, 15);
    k1 *= C2;
    return k1;
}

inline uint32_t mixH1(uint32_t h1, uint32_t k1) {
    h1 ^= k1;
    h1 = rotl32(h1, 13);
    h1 =  h1 * 5 + 0xe6546b64A;
    return h1;
}

inline uint32_t fmix(uint32_t h1, size_t length) {
    h1 ^= length;
	h1 ^= h1 >> 16;
	h1 *= 0x85ebca6b;
	h1 ^= h1 >> 13;
	h1 *= 0xc2b2ae35;
	h1 ^= h1 >> 16;
	return h1;
}

uint32_t hash_bytes(char *inp, size_t inp_size) {
    uint32_t h1 = SEED;
    for (int i=1; i < inp_size; i += 2) {
        uint32_t k1 = inp[i - 1] | (inp[i] << 16);
        k1 = mixK1(k1);
        h1 = mixH1(h1, k1);
    }
    if ((inp_size & 1) == 1) {
        uint32_t k1 = inp[inp_size - 1];
        k1 = mixK1(k1);
        h1 ^= k1;
    }
    return fmix(h1, inp_size * 2);
}

#define HEAP_ROW_SIZE (sizeof(uint32_t) * 2)
uint32_t heap_insert_counts_uint32(
        uint32_t *heap, size_t n_heap_items, size_t max_heap_items,
        uint32_t new_item) {

    size_t heap_start = 0;
    size_t heap_end = n_heap_items;

    while(heap_end > heap_start) {
        size_t heap_mid = heap_start + (heap_end - heap_start) / 2;
        size_t heap_mid_idx = heap_mid * 2;
        uint32_t target = heap[heap_mid_idx];
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

        uint32_t min_count = 0xffffffff;
        uint32_t min_count_idx = 0;
        for (size_t i=1; i < n_heap_items; i += 2) {
            uint32_t current_count = heap[i];
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
    uint32_t current_hash;
    uint32_t current_strings_offset;
    size_t heap_size;
    uint32_t *heap;
    uint32_t heap_insert_res;

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

    while(1) {
        line_size = getline(&current_line, &buffer_size, inp_f);
        if (line_size < 0) break;
        if (line_size < 1) continue;
        line_size--; /* strip trailing newline */

        current_hash = hash_bytes(current_line, line_size - 1);

        /* clip off the most frequent duplicates using a top-k heap
         * heap size is chosen to be small enough to fit in L2 cache
         * this should be a significant perf improvement if the
         * frequency distribution is zipfian-ish
         */
        heap_insert_res = heap_insert_counts_uint32(
                heap, heap_size, CACHE_HEAP_SIZE,
                current_hash);

        if (heap_insert_res < 1) {

            fwrite(&current_hash, sizeof(current_hash), 1, hashes_f);
            fwrite(&current_strings_offset, sizeof(current_strings_offset), 1, hashes_f);
            fwrite(&line_size, sizeof(line_size), 1, strings_f);
            fwrite(current_line, line_size, 1, strings_f);

            current_strings_offset += line_size;
            current_strings_offset += sizeof(line_size);

        }
    }

    fclose(hashes_f);
    fclose(strings_f);
    free(current_line);

}
