#include <stdint.h>

size_t expand_tokens(char *inp, size_t inp_size, char *out_buf, size_t max_outp_size);
void hashes_from_fd(int inp_fd, char *hashes_fname, char *strings_fname);
uint64_t heap_insert_counts_uint64(
        uint64_t *heap, size_t n_heap_items, size_t max_heap_items,
        uint64_t new_item);
uint64_t hash_bytes(char *inp, size_t inp_size);
size_t hash_tokens(char *instring, size_t instring_length,
        uint64_t *outp, 
        size_t *token_offsets,
        size_t *token_lengths,
        size_t outp_length);
size_t tab_col_split_point(char *instring, size_t inp_size, size_t col_idx);
