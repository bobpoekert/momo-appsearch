#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char **argv) {

    if (argc != 2) {
        printf("usage: %s <input fname>\n", argv[0]);
    }
    char *infname = argv[1];
    FILE *infd = fopen(infname, "r");

    int32_t row_scratch[3];
    int32_t current_row = -1;


    while (1) {
        size_t read_size = fread(row_scratch, 4, 3, infd);
        if (read_size < 3) break;
        if (current_row != row_scratch[0]) {
            if (current_row != -1) {
                printf("\r\n");
            }
            current_row = row_scratch[0];
            printf("| ");
        }
        printf("%d:%d ", row_scratch[1], row_scratch[2]);
    }

    fclose(infd);

}
