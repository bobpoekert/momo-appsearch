#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "util.h"
#include "libsstable.h"

#define DEFAULT_BUF_SIZE 4096

/*
 * 1. group lines by app,key
 * 2. run expand_tokens on lines
 * 3. replace all tokens that do not occur in wiki dictionary and
 *  occur in all lines in the group with $$propername%d%%,
 *  where %d is the order in which the name occurs in the english translation
 */

static void buf_set(char **buffer, size_t *buffer_size,
        size_t idx, char *v, size_t v_size) {

    if (idx + v_size > *buffer_size) {
        *buffer = realloc(*buffer, *buffer_size * 2);
        *buffer_size = *buffer_size * 2;
    }

    memcpy(*buffer + idx, v, v_size);
}

static size_t line_key_offset(char *line, size_t line_size) {
    size_t val_offset = 0;
    size_t tab_cnt = 0;
    for (size_t i=0; i < line_size; i++) {
        if (line[i] == '\t') {
            tab_cnt++;
            if (tab_cnt == 2) {
                return i;
            }
        }
    }
    return line_size;
}

int main(int argc, char **argv) {

    if (argc != 3) {
        pritnf("usage: %s <tokens.sstable> <tokens.txt.sstable>\n", argv[0]);
        return 1;
    }

    char *sstable_bin_fname = argv[1];
    char *sstable_txt_fname = argv[2];

    SSTableCtx *sstable = SSTable_open(
            sstable_bin_fname, strlen(sstable_bin_fname),
            sstable_txt_fname, strlen(sstable_txt_fname));

    size_t line_buffer_size = DEFAULT_BUF_SIZE;
    char *line_buffer = malloc(line_buffer_size);
    size_t line_buffer_offset = 0;

    size_t hash_bufffer_size = DEFAULT_BUF_SIZE;
    uint64_t *hash_buffer = malloc(hash_buffer_size);
    size_t hash_buffer_offset = 0;

    size_t hash_buffer_offsets_size = DEFAULT_BUF_SIZE;
    size_t *hash_buffer_offsets = malloc(hash_buffer_offsets_size);
    size_t hash_buffer_offsets_offset = 0;


    size_t uniq_hash_buffer_size = DEFAULT_BUF_SIZE;
    uint64_t *uniq_hash_bufer = malloc(uniq_hash_buffer_size);
    size_t uniq_hash_buffer_offset = 0;

    uint64_t prev_hash = 0;
    uint64_t current_hash;
    size_t line_size;
    size_t current_hash_buffer_size;
    size_t current_line_key_offset;

    size_t current_line_buffer_size = DEFAULT_BUF_SIZE;
    char *current_line_buffer = malloc(current_line_buffer_size);


    while(1) {
        line_size = getline(&current_line_buffer, &current_line_buffer_size, stdin);
        if (line_size < 0) break;
        if (line_size < 1) continue;

        current_line_key_offset = line_key_offset(current_line_buffer, current_line_buffer_size);
        current_hash = hash_bytes(current_line_buffer, current_line_key_offset);

        if (current_hash != prev_hash) {
            hash_buffer_offsets[hash_buffer_offsets_offset] = hash_buffer_offset;
            hash_buffer_offsets_offset++;

            /* detect propernames */
        }


        current_hash_buffer_size = hash_tokens(
                currnet_line_buffer, line_size,
                &(hash_buffer[hash_buffer_offset]), hash_buffer_offsets_size - hash_buffer_offset);

        current_hash_buffer_

        for (size_t hash_idx = current_hash_buffer_offset; 


    }

}
