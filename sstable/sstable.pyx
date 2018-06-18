from libc.stdint cimport *
from libc.stdio cimport fopen, fdopen, fread, fclose, fwrite, FILE, ferror, perror, feof, fseek, SEEK_SET, getline, SEEK_CUR
from libc.stdlib cimport malloc, free, realloc

cdef extern from "stdio.h":
    ssize_t getdelim(char **lineptr, size_t *n, int delim, FILE *stream)

import numpy as np
cimport numpy as np
from cython cimport view
import os
import codecs

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
    cdef np.ndarray[np.uint32_t, ndim=1] raw_inp_offsets = hash_offsets[:, 1]
    cdef np.ndarray[np.uint32_t, ndim=1] raw_hashes = hash_offsets[:, 0]
    cdef np.ndarray[long, ndim=1] sort_indexes = np.argsort(raw_hashes)

    cdef np.ndarray[np.uint32_t, ndim=1] sorted_inp_offsets = raw_inp_offsets[sort_indexes]
    cdef np.ndarray[np.uint32_t, ndim=1] sorted_hashes = raw_hashes[sort_indexes]

    cdef np.ndarray dupe_mask = np.zeros(n_items, dtype=np.bool)

    cdef size_t idx = 0
    cdef size_t max_idx = n_items
    cdef uint32_t prev_hash = 0
    cdef uint32_t cur_hash

    while idx < max_idx:
        cur_hash = sorted_hashes[idx]
        if cur_hash == prev_hash:
            dupe_mask[idx] = 0
        else:
            dupe_mask[idx] = 1
        prev_hash = cur_hash
        idx += 1

    _uniq_hashes = sorted_hashes[dupe_mask]
    cdef np.ndarray[np.uint32_t, ndim=1] uniq_hashes = _uniq_hashes
    cdef np.ndarray[np.uint32_t, ndim=1] uniq_offsets = sorted_inp_offsets[dupe_mask]
    cdef np.ndarray[np.uint32_t, ndim=2] outp_offsets = np.zeros((_uniq_hashes.shape[0], 2), dtype=np.uint32)

    print _uniq_hashes.shape, np.nonzero(dupe_mask)[0].shape

    cdef size_t current_offset = 0
    cdef size_t current_idx = 0
    cdef size_t outp_idx = 0
    cdef uint32_t line_size = 0
    cdef FILE *strings_tempfile = fopen(strings_tempname_c, "r")
    cdef FILE *strings_outfile = fopen(strings_outname_c, "w")
    cdef char *line_buf = <char *> malloc(1024)
    cdef char *line_buf_cursor = line_buf
    cdef size_t line_buf_size = 1024
    cdef size_t read_line_size
    cdef size_t line_size_cursor

    if line_buf == NULL:
        raise MemoryError()

    try:
        while current_idx < max_idx:
            if dupe_mask[current_idx] == 1:

                fseek(strings_tempfile, uniq_offsets[outp_idx], SEEK_SET)
                fread(&line_size, sizeof(line_size), 1, strings_tempfile)

                print line_size

                if line_buf_size < line_size:
                    line_buf = <char *> realloc(line_buf, line_size)
                    line_buf_size = line_size

                fseek(strings_tempfile, sizeof(line_size), SEEK_CUR);
                fread(line_buf, line_size, 1, strings_tempfile)
                fwrite(line_buf, line_size, 1, strings_outfile)

                outp_offsets[outp_idx, 0] = current_offset
                outp_offsets[outp_idx, 1] = line_size
                current_offset += line_size

                outp_idx += 1
            current_idx += 1
    finally:
        fclose(strings_tempfile)
        fclose(strings_outfile)
        free(line_buf)

    uniq_sort_indexes = sort_indexes[dupe_mask]
    np.concatenate((uniq_hashes, outp_offsets.flatten())).tofile(hashes_outname)


def build_mat_index(_hashes, _values, _hashes_fname, _strings_fname):
    """
    takes an array of hashes and a matrix with the same height of values
    and generates an sstable index of hashes -> matrix rows
    """
    cdef np.ndarray[np.uint32_t, ndim=1] hashes = _hashes
    cdef np.ndarray[long, ndim=1] sort_indexes = np.argsort(hashes)

    cdef np.ndarray[np.uint32_t, ndim=1] sorted_hashes = hashes[sort_indexes]
    cdef np.ndarray sorted_values = _values[sort_indexes]

    cdef char *hashes_fname = _hashes_fname
    cdef char *strings_fname = _strings_fname
    cdef size_t n_rows = _hashes.shape[0]

    cdef size_t row_idx = 0
    cdef uint32_t current_hash
    cdef uint32_t prev_hash = 0
    cdef size_t hash_idx = 0
    cdef uint32_t current_offset = 0
    cdef np.ndarray[np.uint32_t, ndim=1] offsets = np.zeros((n_rows,), dtype=np.uint32)
    cdef np.ndarray[np.uint32_t, ndim=1] uniq_hashes = np.zeros((n_rows,), dtype=np.uint32)
    cdef np.ndarray row
    cdef uint32_t[::1] row_buf
    cdef size_t row_size

    cdef FILE *strings_outf = fopen(strings_fname, "w")
    try:
        while row_idx < n_rows:
            current_hash = sorted_hashes[row_idx]
            if current_hash != prev_hash:
                uniq_hashes[hash_idx] = prev_hash
                offsets[hash_idx] = current_offset
                hash_idx += 1
            row = sorted_values[row_idx]
            if not row.flags['C_CONTIGUOUS']:
                row = np.ascontiguousarray(row)
            row_buf = row
            row_size = row.size
            fwrite(&row_buf[0], sizeof(uint32_t), row_size, strings_outf)
            current_offset += row_size
            row_idx += 1
            prev_hash = current_hash

        uniq_hashes[hash_idx] = prev_hash
        offsets[hash_idx] = current_offset
    finally:
        fclose(strings_outf)

    np.concatenate((uniq_hashes[:hash_idx], offsets[:hash_idx])).tofile(_strings_fname)


class SSTable(object):

    def __init__(self, hashes_fname, strings_fname):
        self.hashes_file = np.memmap(hashes_fname, dtype=np.uint32)
        self.hashes_length = self.hashes_file.shape[0] / 3
        self.hashes = self.hashes_file[:self.hashes_length]
        self.offsets = self.hashes_file[self.hashes_length:].reshape((-1, 2))
        self.strings = open(strings_fname, 'r')

    def decode(self, row):
        return row.decode('utf-8')

    def get(self, k, default=None):
        hash_idx = np.searchsorted(self.hashes, k)
        if self.hashes[hash_idx] != k:
            return default
        offset = self.offsets[hash_idx, 0]
        self.strings.seek(offset)
        length = self.offsets[hash_idx, 1]
        return self.decode(self.strings.read(length))

    def itervalues(self):
        idx = 0
        cursor = 0
        while idx < self.hashes_length:
            self.strings.seek(cursor)
            if idx < self.hashes_length - 1:
                length = self.offsets[cursor + 1] - cursor
                yield self.decode(self.strings.read(length))
            else:
                yield self.decode(self.strings.read())
            idx += 1
            cursor += length

    def values(self):
        return list(self.iteravlues())

    def keys(self):
        return self.hashes

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

class MatSSTable(SSTable):

    def __init__(self, *args, dtype=None):
        assert dtype is not None
        self.dtype = dtype
        SSTable.__init__(self, *args)

    def decode(self, blob):
        return np.frombuffer(blob, dtype=self.dtype)
