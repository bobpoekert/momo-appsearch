#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

size_t expand_tokens(char *inp, size_t inp_size, char *out_buf, size_t max_outp_size) {

    size_t inp_idx = 0;
    size_t outp_idx = 0;
    char in_whitespace = 0;

    while ((inp_idx < inp_size) && (outp_idx < max_outp_size)) {
        uint32_t c = 0;
        char *c_parts = (char *) &c;
        size_t char_length = 0;
        if ((inp_size - inp_idx > 4) &&
            ((inp[inp_idx] & 0xF800) == 0xF000) &&
            ((inp[inp_idx + 1] & 0xC000) == 0x8000) &&
            ((inp[inp_idx + 2] & 0xC000) == 0x8000) &&
            ((inp[inp_idx + 3] & 0xC000) == 0x8000)) {
            c = *((uint32_t *) (inp + inp_idx));
            char_length = 4;
        } else if ((inp_size - inp_idx > 3) &&
                   ((inp[inp_idx] & 0xF000) == 0xE000) &&
                   ((inp[inp_idx + 1] & 0xC000) == 0x8000) &&
                   ((inp[inp_idx + 2] & 0xC000) == 0x8000)) {
            c_parts[2] = inp[inp_idx];
            c_parts[1] = inp[inp_idx + 1];
            c_parts[0] = inp[inp_idx + 2];
            char_length = 3;
        } else if ((inp_size - inp_idx > 2) &&
                    ((inp[inp_idx] & 0xE000) == 0xC000) &&
                    ((inp[inp_idx + 1] & 0xC000) == 0x8000)) {
            c_parts[1] = inp[inp_idx];
            c_parts[0] = inp[inp_idx + 1];
            char_length = 2;
        } else if ((inp[inp_idx] & 0x8000) == 0) {
            c_parts[0] = inp[inp_idx];
            char_length = 1;
        } else {
            return 0; /* invalid char length! should be impossible */
        }
        inp_idx += char_length;
        
        
        if ((c >= 0x0041 && c <= 0x005A)) { /* A-Z */
            in_whitespace = 0;
            out_buf[outp_idx] = c_parts[0] + 0x20; /* convert to lowercase */
            outp_idx++;
        } else if ((c >= 0x0030 && c <= 0x0039) || /* 0-9 */
                   c == 0x27 || c == 0x2c || /* apostraphe */
                   c == '&' || c == '#' || c == ';' || /* character entities */
                   (c >= 0x0061 && c <= 0x007A)) { /* a-z */
            in_whitespace = 0;
            out_buf[outp_idx] = c_parts[0];
            outp_idx++;
        } else if (
               /*SPACE*/                     c == 0x20||
               /*NO-BREAK SPACE*/            c == 0xc2a0||
               /*OGHAM SPACE MARK*/          c == 0xe19a80||
               /*EN QUAD*/                   c == 0xe28080||
               /*EM QUAD*/                   c == 0xe28081||
               /*EN SPACE*/                  c == 0xe28082||
               /*EM SPACE*/                  c == 0xe28083||
               /*THREE-PER-EM SPACE*/        c == 0xe28084||
               /*FOUR-PER-EM SPACE*/         c == 0xe28085||
               /*SIX-PER-EM SPACE*/          c == 0xe28086||
               /*FIGURE SPACE*/              c == 0xe28087||
               /*PUNCTUATION SPACE*/         c == 0xe28088||
               /*THIN SPACE*/                c == 0xe28089||
               /*HAIR SPACE*/                c == 0xe2808a||
               /*ZERO WIDTH SPACE*/          c == 0xe2808b||
               /*NARROW NO-BREAK SPACE*/     c == 0xe280af||
               /*MEDIUM MATHEMATICAL SPACE*/ c == 0xe2819f||
               /*IDEOGRAPHIC SPACE*/         c == 0xe38080||
               /*TAB*/                       c == 0x09||
               /*NUL*/                       c == 0x00||
               /*newline*/                   c == 0xa||
               /*vertical tab*/              c == 0xb||
               /*carriage return*/           c == 0xd
                ){ /* whitespace */
            if (!in_whitespace) {
                out_buf[outp_idx] = ' ';
                outp_idx++;
                in_whitespace = 1;
            }
        } else { /* punctuation or non-ascii */
            if (!in_whitespace) {
                out_buf[outp_idx] = ' ';
                outp_idx++;
            }
            if (outp_idx + char_length >= max_outp_size) break;
            for (size_t i=0; i < char_length; i++) {
                out_buf[outp_idx] = c_parts[i];
                outp_idx++;
            }
            if (outp_idx >= max_outp_size) break;
            out_buf[outp_idx] = ' ';
            in_whitespace = 1;
            outp_idx++;
        }
    }

    return outp_idx;

}

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
