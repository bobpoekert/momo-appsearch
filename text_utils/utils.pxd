from libc.stdint cimport *

cdef extern from "util.h":

    size_t expand_tokens(char *inp, size_t inp_size, char *out_buf, size_t max_outp_size)
    size_t hash_tokens(char *instring, size_t instring_length,
            uint64_t *outp,
            size_t *token_offsets,
            size_t *token_lengths,
            size_t outp_length)
    uint64_t hash_bytes(char *inp, size_t inp_size);
    size_t tab_col_split_point(char *instring, size_t inp_size, size_t col_idx)


