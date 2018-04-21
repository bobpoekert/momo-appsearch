#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include "quadtree.h"

#define mmap_open(fd, size, flag) mmap(0, size, flag, MAP_PRIVATE, fd, 0)

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("usage: %s <inp fname> <outp fname>\n", argv[0]);
        return 1;
    }
    char *inp_fname = argv[1];
    char *outp_fname = argv[2];

    struct stat inp_stat;
    stat(inp_fname, &inp_stat);

    int inp_fd = open(inp_fname, O_RDWR);
    uint32_t *inp_data = mmap_open(inp_fd, inp_stat.st_size, PROT_READ);

    size_t n_rows = inp_stat.st_size / (sizeof(uint32_t) * 2);
    quadtree__tree *tree = quadtree__init(n_rows, inp_data);
    uint32_t *sorted_indexes = malloc(n_rows * sizeof(uint32_t));
    quadtree__sort(tree, sorted_indexes);

    munmap(inp_data, inp_stat.st_size);
    close(inp_fd);

    size_t outp_size = sizeof(uint64_t) * 4 + sizeof(uint64_t) * n_rows;
    truncate(outp_fname, outp_size);
    int outp_fd = open(outp_fname, O_RDWR | O_CREAT);
    if (outp_fd < 0) {
        printf("failed to open output file\n");
        return 1;
    }
    uint64_t header[4];

    header[0] = tree->max_x;
    header[1] = tree->min_x;
    header[2] = tree->max_y;
    header[3] = tree->min_y;

    write(outp_fd, header, sizeof(uint64_t) * 4);

    uint64_t *sorted_outp = malloc(sizeof(uint64_t) * n_rows);

    for (size_t outp_idx=0; outp_idx < n_rows; outp_idx++) {
        sorted_outp[outp_idx] = tree->rows[sorted_indexes[outp_idx]];
    }

    write(outp_fd, sorted_outp, sizeof(uint64_t) * n_rows);
    close(outp_fd);

}