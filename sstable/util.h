#include <stdint.h>

void hashes_from_fd(int inp_fd, char *hashes_fname, char *strings_fname);
uint64_t heap_insert_counts_uint64(
        uint64_t *heap, size_t n_heap_items, size_t max_heap_items,
        uint64_t new_item);
uint64_t hash_bytes(char *inp, size_t inp_size);
