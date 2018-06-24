#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char **argv) {

    if (argc != 4) {
        printf("usage: %s <sample percentage> <input fname> <output fname>\n", argv[0]);
        return 1;
    }

    long percentage = atol(argv[1]);
    char *infname = argv[2];
    char *outfname = argv[3];

    FILE *infd = fopen(infname, "r");
    FILE *outfd = fopen(outfname, "w");

    int32_t row_scratch[3];
    int32_t row_ctr = -1;
    int32_t current_row = -1;

    while (1) {
        size_t read_size = fread(row_scratch, 4, 3, infd);
        if (read_size < 3) break;
        if (row_scratch[0] % 100 >= percentage) continue;
        if (current_row != row_scratch[0]) {
            current_row = row_scratch[0];
            row_ctr++;
        }
        row_scratch[0] = row_ctr;
        if (fwrite(row_scratch, 4, 3, outfd) < 3) break;
    }

    fclose(infd);
    fclose(outfd);


}
