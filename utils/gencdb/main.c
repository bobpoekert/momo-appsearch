#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include "murmur3.c"
#include "../mcdb/mcdb.h"
#include "../mcdb/mcdb_make.h"
#include "../mcdb/mcdb_error.h"

#define SEED 0xdeadbeef

int main(int argc, char **argv) {

    char *current_line;
    int line_size;
    size_t buffer_size;
    uint64_t current_hash[2];
    int outf_fd;
    struct mcdb_make m;
   
    if (argc != 2) {
        printf("usage: %s <database filename> (reads input from stdin)\n", argv[0]);
        return 1;
    }

    outf_fd = open(argv[1], O_RDWR|O_CREAT, 0666);

    if (outf_fd == -1) {
        fprintf(stderr, "failed to open output file\n");
        return 1;
    }

    if (mcdb_make_start(&m, outf_fd, malloc, free) != 0) {
        mcdb_error(MCDB_ERROR_WRITE, "init", "");
        return 1;
    }

    buffer_size = 1024 * 1024;
    current_line = malloc(buffer_size);

    while(1) {
        line_size = getline(&current_line, &buffer_size, stdin);
        if (line_size < 1) {
            break;
        }
        
        line_size--; /* drop trailing newline */
        MurmurHash3_x64_128(current_line, line_size, SEED, current_hash);

        if (mcdb_make_add(&m,
                    (char *) current_hash, 16,
                    current_line, line_size) < 0) {
            mcdb_error(MCDB_ERROR_WRITE, "write", "");
            return 1;
        }
    }

    if (mcdb_make_finish(&m) < 1) {
        mcdb_error(MCDB_ERROR_WRITE, "finish", "");
        return 1;
    }

}
