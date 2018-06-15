from libc.stdint cimport *
from libc.stdio cimport fdopen, fread, fclose, fwrite, FILE
from libc.stdlib cimport malloc, free

cdef extern from "stdio.h":
    ssize_t getdelim(char **lineptr, size_t *n, int delim, FILE *stream)

import numpy as np
cimport numpy as np

cdef extern from "util.h":
    void hashes_from_fd(int inp_fd, int hashes_fd, int strings_fd)

def build_index(infile, hashes_tempfile, hashes_outfile, strings_tempfile, strings_outfile):

    hashes_from_fd(infile.fileno(), hashes_tempfile.fileno(), strings_tempfile.fileno())

    hash_offsets = np.memmap(hashes_tempfile, dtype=np.uint32).reshape((-1, 2))
    cdef size_t n_items = hash_offsets.shape[0]
    cdef np.ndarray[np.uint32_t, ndim=1] hashes = hash_offsets[:, 0]
    cdef np.ndarray[np.uint64_t, ndim=1] sort_indexes = np.argsort(hashes)

    cdef np.ndarray[np.npy_bool, ndim=1] dupe_mask = np.zeros(n_items, dtype=np.bool)

    cdef size_t idx = 0
    cdef size_t max_idx = n_items
    cdef uint32_t prev_hash = 0
    cdef uint32_t cur_hash

    while idx < max_idx:
        cur_hash = hashes[sort_indexes[idx]]
        if cur_hash == prev_hash:
            dupe_mask[idx] = 0
        else:
            dupe_mask[idx] = 1
        prev_hash = cur_hash
        idx += 1

    uniq_hashes = hashes[dupe_mask]
    uniq_offsets = np.zeros(uniq_hashes.shape, dtype=np.uitn32)

    cdef size_t current_offset = 0
    cdef size_t current_idx = 0
    cdef size_t outp_idx = 0
    cdef ssize_t line_size = 0
    cdef FILE *cfile = fdopen(strings_tempfile.fileno(), "r")
    cdef FILE *outfile = fdopen(strings_outfile.fileno(), "w")
    cdef char *line_buf = <char *> malloc(1024)
    cdef size_t line_buf_size = 1024

    try:
        while 1:
            line_size = getdelim(&line_buf, &line_buf_size, 0, cfile)
            if line_size < 0:
                break
            if dupe_mask[current_idx]:
                fwrite(line_buf, line_size, 1, outfile)
                uniq_offsets[outp_idx] = current_offset
                current_offset += line_size
                outp_idx += 1
            current_idx += 1
    finally:
        fclose(cfile)
        fclose(outfile)
        free(line_buf)

    np.concatenate((uniq_hashes, uniq_offsets)).tofile(hashes_outfile)
