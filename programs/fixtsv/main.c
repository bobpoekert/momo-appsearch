#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

int main(int argc, char **argv) {

    uint8_t tab_count = 0;
    char current_char;


    while(1) {
        current_char = getchar();
        if (current_char == EOF) break;
        if (current_char == '\t') {
            tab_count++;
            putchar(current_char);
        } else if (current_char == '\n') {
            if (tab_count == 3) {
                putchar(current_char);
                tab_count = 0;
            } else {
                putchar('\\');
                putchar('n');
            }
        } else {
            putchar(current_char);
        }
    }

}
