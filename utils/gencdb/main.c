#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include "../mcdb/mcdb.h"
#include "../mcdb/mcdb_make.h"
#include "../mcdb/mcdb_error.h"


/* transliterated for comaptibility from: https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/Murmur3.java */

#define SEED 0
#define C1 0xcc9e2d51
#define C2 0x1b873593

inline uint32_t rotl32 ( uint32_t x, int8_t r )
{
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
    if (inp_size & 1 == 1) {
        uint32_t k1 = inp[inp_size - 1];
        k1 = mixK1(k1);
        h1 ^= k1;
    }
    return fmix(h1, inp_size * 2);
}

int main(int argc, char **argv) {

    char *current_line;
    int line_size;
    size_t buffer_size;
    uint32_t current_hash;
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
        current_hash = hash_bytes(current_line, line_size);

        if (mcdb_make_add(&m,
                    (char *) &current_hash, sizeof(uint32_t),
                    current_line, line_size) < 0) {
            mcdb_error(MCDB_ERROR_WRITE, "write", "");
            return 1;
        }
    }

    if (mcdb_make_finish(&m) < 1) {
        mcdb_error(MCDB_ERROR_WRITE, "finish", "");
    }
    return 0;

}
