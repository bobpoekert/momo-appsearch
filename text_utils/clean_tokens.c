#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

size_t expand_tokens(char *inp, size_t inp_size, char *out_buf, size_t max_outp_size) {

    size_t inp_idx = 0;
    size_t outp_idx = 0;
    char in_whitespace = 0;

    while ((inp_idx < inp_size) && (outp_idx < max_outp_size)) {
        uint32_t c = 0;
        char *c_parts = (char *) &c;
        size_t char_length = 0;
        if ((inp_size - inp_idx > 4) &&
            ((inp[inp_idx] & 0xF800) == 0xF000) &&
            ((inp[inp_idx + 1] & 0xC000) == 0x8000) &&
            ((inp[inp_idx + 2] & 0xC000) == 0x8000) &&
            ((inp[inp_idx + 3] & 0xC000) == 0x8000)) {
            c = *((uint32_t *) (inp + inp_idx));
            char_length = 4;
        } else if ((inp_size - inp_idx > 3) &&
                   ((inp[inp_idx] & 0xF000) == 0xE000) &&
                   ((inp[inp_idx + 1] & 0xC000) == 0x8000) &&
                   ((inp[inp_idx + 2] & 0xC000) == 0x8000)) {
            c_parts[2] = inp[inp_idx];
            c_parts[1] = inp[inp_idx + 1];
            c_parts[0] = inp[inp_idx + 2];
            char_length = 3;
        } else if ((inp_size - inp_idx > 2) &&
                    ((inp[inp_idx] & 0xE000) == 0xC000) &&
                    ((inp[inp_idx + 1] & 0xC000) == 0x8000)) {
            c_parts[1] = inp[inp_idx];
            c_parts[0] = inp[inp_idx + 1];
            char_length = 2;
        } else if ((inp[inp_idx] & 0x8000) == 0) {
            c_parts[0] = inp[inp_idx];
            char_length = 1;
        } else {
            return 0; /* invalid char length! should be impossible */
        }
        inp_idx += char_length;
        
        
        if ((c >= 0x0041 && c <= 0x005A)) { /* A-Z */
            in_whitespace = 0;
            out_buf[outp_idx] = c_parts[0] + 0x20; /* convert to lowercase */
            outp_idx++;
        } else if ((c >= 0x0030 && c <= 0x0039) || /* 0-9 */
                   c == 0x27 || c == 0x2c || /* apostraphe */
                   c == '&' || c == '#' || c == ';' || /* character entities */
                   (c >= 0x0061 && c <= 0x007A)) { /* a-z */
            in_whitespace = 0;
            out_buf[outp_idx] = c_parts[0];
            outp_idx++;
        } else if (
               /*SPACE*/                     c == 0x20||
               /*NO-BREAK SPACE*/            c == 0xc2a0||
               /*OGHAM SPACE MARK*/          c == 0xe19a80||
               /*EN QUAD*/                   c == 0xe28080||
               /*EM QUAD*/                   c == 0xe28081||
               /*EN SPACE*/                  c == 0xe28082||
               /*EM SPACE*/                  c == 0xe28083||
               /*THREE-PER-EM SPACE*/        c == 0xe28084||
               /*FOUR-PER-EM SPACE*/         c == 0xe28085||
               /*SIX-PER-EM SPACE*/          c == 0xe28086||
               /*FIGURE SPACE*/              c == 0xe28087||
               /*PUNCTUATION SPACE*/         c == 0xe28088||
               /*THIN SPACE*/                c == 0xe28089||
               /*HAIR SPACE*/                c == 0xe2808a||
               /*ZERO WIDTH SPACE*/          c == 0xe2808b||
               /*NARROW NO-BREAK SPACE*/     c == 0xe280af||
               /*MEDIUM MATHEMATICAL SPACE*/ c == 0xe2819f||
               /*IDEOGRAPHIC SPACE*/         c == 0xe38080||
               /*TAB*/                       c == 0x09||
               /*NUL*/                       c == 0x00||
               /*newline*/                   c == 0xa||
               /*vertical tab*/              c == 0xb||
               /*carriage return*/           c == 0xd
                ){ /* whitespace */
            if (!in_whitespace) {
                out_buf[outp_idx] = ' ';
                outp_idx++;
                in_whitespace = 1;
            }
        } else { /* punctuation or non-ascii */
            if (!in_whitespace) {
                out_buf[outp_idx] = ' ';
                outp_idx++;
            }
            if (outp_idx + char_length >= max_outp_size) break;
            for (size_t i=0; i < char_length; i++) {
                out_buf[outp_idx] = c_parts[i];
                outp_idx++;
            }
            if (outp_idx >= max_outp_size) break;
            out_buf[outp_idx] = ' ';
            in_whitespace = 1;
            outp_idx++;
        }
    }

    return outp_idx;

}

#if 0
int main(int argc, char **argv) {
    char *inbuf = malloc(1024);
    char *outbuf = malloc(1024);
    size_t inp_size;
    size_t outp_size;
    while (1) {
        inp_size = getline(&inbuf, &inp_size, stdin);
        outp_size = expand_tokens(inbuf, inp_size, outbuf, 1024);
        for (size_t i=0; i < outp_size; i++) {
            putchar(outbuf[i]);
        }
        putchar('\n');
    }
}
#endif
