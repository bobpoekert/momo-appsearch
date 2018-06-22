from libc.stdint cimport *

cdef extern from "util.h":

    size_t expand_tokens(char *inp, size_t inp_size, char *out_buf, size_t max_outp_size)
    size_t hash_tokens(char *, size_t, uint64_t *, size_t)

