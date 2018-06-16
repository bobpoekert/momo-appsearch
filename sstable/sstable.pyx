from libc.stdint cimport *
from libc.stdio cimport fopen, fdopen, fread, fclose, fwrite, FILE
from libc.stdlib cimport malloc, free, realloc

cdef extern from "stdio.h":
    ssize_t getdelim(char **lineptr, size_t *n, int delim, FILE *stream)

import numpy as np
cimport numpy as np
import os

cdef extern from "util.h":
    void hashes_from_fd(int inp_fd, char *hashes_fname, char *strings_fname)

def build_index(infile, hashes_tempname, hashes_outname, strings_tempname, strings_outname):

    cdef char *hashes_tempname_c = hashes_tempname
    cdef char *hashes_outname_c = hashes_outname
    cdef char *strings_tempname_c = strings_tempname
    cdef char *strings_outname_c = strings_outname

    hashes_from_fd(infile.fileno(), hashes_tempname_c, strings_tempname_c)

    hash_offsets = np.memmap(hashes_tempname, dtype=np.uint32).reshape((-1, 2))
    cdef size_t n_items = hash_offsets.shape[0]
    cdef np.ndarray[np.uint32_t, ndim=1] inp_offsets = hash_offsets[:, 1]
    cdef np.ndarray[np.uint32_t, ndim=1] hashes = hash_offsets[:, 0]
    cdef np.ndarray[long, ndim=1] sort_indexes = np.argsort(hashes)

    cdef np.ndarray[np.npy_bool, ndim=1] dupe_mask = np.zeros(n_items, dtype=np.uint8)

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

    _uniq_hashes = hashes[dupe_mask]
    cdef np.ndarray[np.uint32_t, ndim=1] uniq_hashes = _uniq_hashes
    cdef np.ndarray[np.uint32_t, ndim=1] uniq_offsets = np.zeros(_uniq_hashes.shape, dtype=np.uint32)

    cdef size_t current_offset = 0
    cdef size_t current_idx = 0
    cdef size_t outp_idx = 0
    cdef ssize_t line_size = 0
    cdef FILE *cfile = fopen(strings_tempname_c, "r")
    cdef FILE *outfile = fopen(strings_outname_c, "w")
    cdef char *line_buf = <char *> malloc(1024)
    cdef size_t line_buf_size = 1024

    if line_buf == NULL:
        raise MemoryError()

    try:
        while 1:
            line_size = getdelim(&line_buf, &line_buf_size, 0, cfile)
            if current_idx > 0:
                line_size = inp_offsets[current_idx] - inp_offsets[current_idx - 1]
            else:
                line_size = inp_offsets[current_idx + 1]

            if line_buf_size < line_size:
                line_size = max(line_size * 2, line_buf_size)
                line_buf = <char *> realloc(line_buf, line_size)
                if line_buf == NULL:
                    raise MemoryError()

            if fread(line_buf, line_size, 1, cfile) < line_size:
                raise IOError('Failed to read temporary strings file')

            if dupe_mask[current_idx] != 0:
                if fwrite(line_buf, line_size, 1, outfile) < line_size:
                    raise IOError('Failed to write to strings file')
                uniq_offsets[outp_idx] = current_offset
                current_offset += line_size
                outp_idx += 1
            current_idx += 1
    finally:
        fclose(cfile)
        fclose(outfile)
        free(line_buf)

    uniq_sort_indexes = sort_indexes[dupe_mask]
    np.concatenate((uniq_hashes[uniq_sort_indexes], uniq_offsets[uniq_sort_indexes])).tofile(hashes_outname)

class SSTable(object):

    def __init__(self, hashes_fname, strings_fname):
        self.hashes_file = np.memmap(hashes_fname, dtype=np.uint32)
        self.hashes_length = self.hashes_file.shape[0] / 2
        self.hashes = self.hashes_file[:self.hashes_length]
        self.offsets = self.hashes_file[self.hashes_length:]
        self.strings = open(strings_fname, 'r')

    def get(self, k, default=None):
        hash_idx = np.searchsorted(self.hashes, k)[0]
        if self.hashes[hash_idx] != k:
            return default
        offset = self.offsets[hash_idx]
        self.strings.seek(offset)
        if hash_idx < self.hashes_length:
            length = self.offsets[hash_idx + 1] - offset
            return self.strings.read(length)
        else:
            return self.strings.read()

    def __getitem__(self, k):
        res = self.get(k)
        if res == None:
            raise KeyError('%d' % k)
        return res

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.strings.close()
        return False
