#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char **argv) {

    uint8_t tab_count = 0;
    char current_char;
    size_t n_cols = argc-1;
    uint32_t *targets;
    char tab_on = 0;

    if (argc < 2) {
        printf("usage: %s <column id> [<column id>, ...]\n", argv[0]);
        return 1;
    }

    targets = malloc(sizeof(uint32_t) * n_cols);

    for (size_t i=0; i < n_cols; i++) {
        targets[i] = atoi(argv[i+1]);
    }

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
        current_char = getchar();
        if (current_char == EOF) break;
        if (current_char == '\t') {
            if (tab_on) {
                putchar('\t');
            }
            tab_count++;
            UPDATE_TAB_ON
        } else if (current_char == '\n') {
            tab_count = 0;
            UPDATE_TAB_ON
            putchar('\n');
        } else if (tab_on) {
            putchar(current_char);
        }
    }

}
