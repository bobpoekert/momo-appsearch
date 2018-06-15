#include <stdint.h>

void hashes_from_fd(int inp_fd, int hashes_fd, int strings_fd);
uint32_t heap_insert_counts_uint32(
        uint32_t *heap, size_t n_heap_items, size_t max_heap_items,
        uint32_t new_item);
