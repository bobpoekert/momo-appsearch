#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

#define BUFFER_SIZE 1048576

int main(int argc, char **argv) {

    uint8_t tab_count = 0;
    char current_char;
    size_t n_cols = argc-1;
    uint32_t *targets;
    char tab_on = 0;
    
    char *read_buffer;
    size_t read_offset = 0;
    size_t read_end = 0;

    char *write_buffer;
    size_t write_offset;

    if (argc < 2) {
        printf("usage: %s <column id> [<column id>, ...]\n", argv[0]);
        return 1;
    }

    targets = malloc(sizeof(uint32_t) * n_cols);
    read_buffer = malloc(BUFFER_SIZE);
    write_buffer = malloc(BUFFER_SIZE);


    for (size_t i=0; i < n_cols; i++) {
        targets[i] = atoi(argv[i+1]);
    }

#define WRITE_CHAR(c) \
    if (write_offset >= BUFFER_SIZE) {\
        if (write(1, write_buffer, BUFFER_SIZE) < 1) break;\
        write_offset = 0;\
    }\
    write_buffer[write_offset] = c;\
    write_offset++;


#define UPDATE_TAB_ON \
        tab_on = 0;\
        for (size_t i=0; i < n_cols; i++) {\
            if (targets[i] == tab_count) {\
                tab_on = 1;\
                break;\
            }\
        }

    UPDATE_TAB_ON
    while(1) {
        read_offset++;
        if (read_offset >= read_end) {
            read_end = read(0, read_buffer, BUFFER_SIZE);
            if (read_end < 1) break;
            read_offset = 0;
        }
        current_char = read_buffer[read_offset];
        if (current_char == '\t') {
            if (tab_on) {
                WRITE_CHAR('\t')
            }
            tab_count++;
            UPDATE_TAB_ON
        } else if (current_char == '\n') {
            tab_count = 0;
            UPDATE_TAB_ON
            WRITE_CHAR('\n')
        } else if (tab_on) {
            WRITE_CHAR(current_char)
        }
    }

    if (write_offset > 0) {
        if (write(1, write_buffer, write_offset) < 1) return 1;
    }

}
