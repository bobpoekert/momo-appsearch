from libc.stdint cimport *
from libc.stdio cimport fopen, fdopen, fread, fclose, fwrite, FILE, ferror, perror, feof, fseek, SEEK_SET, getline, SEEK_CUR, ftell
from libc.stdlib cimport malloc, free, realloc
from libc.string cimport strerror
import os

cdef extern from "stdio.h":
    ssize_t getdelim(char **lineptr, size_t *n, int delim, FILE *stream)

cdef extern from "sys/mman.h":
    void *mmap(void *, size_t, int, int, int, size_t)
    int munmap(void *addr, size_t size)
    cdef enum:
        PROT_READ "PROT_READ"
        MAP_SHARED "MAP_SHARED"
        MAP_FAILED "MAP_FAILED"
        MAP_PRIVATE "MAP_PRIVATE"

cdef extern from "errno.h":
    int errno

import numpy as np
cimport numpy as np
from cython cimport view
import os
import codecs

cdef extern from "util.h":
    void hashes_from_fd(int inp_fd, char *hashes_fname, char *strings_fname)
    uint32_t hash_bytes(char *inp, size_t inp_size)

def hash_string(s):
    cdef bytes py_bytes = s.encode()
    cdef char *cstr = py_bytes
    return hash_bytes(cstr, len(py_bytes))

def _hashes_from_fd(infile, hashes_tempname, strings_tempname):
    cdef char *hashes_tempname_c = hashes_tempname
    cdef char *strings_tempname_c = strings_tempname

    hashes_from_fd(infile.fileno(), hashes_tempname_c, strings_tempname_c)

def duplicate_mask(_sorted_array):
    cdef np.ndarray sorted_array = _sorted_array
    cdef size_t n_items = _sorted_array.shape[0]
    cdef np.ndarray dupe_mask = np.zeros(n_items, dtype=np.bool)

    cdef size_t idx = 0
    cdef uint32_t prev_hash = 0
    cdef uint32_t cur_hash

    while idx < n_items:
        cur_hash = sorted_array[idx]
        if cur_hash == prev_hash:
            dupe_mask[idx] = 0
        else:
            dupe_mask[idx] = 1
        prev_hash = cur_hash
        idx += 1

    # first element is never a duplicate
    dupe_mask[0] = 1

    return dupe_mask

def sort_uniqify_hashes(hashes, offsets):
    cdef np.ndarray[long, ndim=1] sort_indexes = np.argsort(hashes)
    cdef np.ndarray sorted_hashes = hashes[sort_indexes]
    cdef np.ndarray dupe_mask = duplicate_mask(sorted_hashes)
    cdef np.ndarray sorted_offsets = offsets[sort_indexes]
    return (sorted_hashes[dupe_mask], sorted_offsets[dupe_mask])

def build_index(infile, hashes_tempname, hashes_outname, strings_tempname, strings_outname):

    cdef char *hashes_tempname_c = hashes_tempname
    cdef char *hashes_outname_c = hashes_outname
    cdef char *strings_tempname_c = strings_tempname
    cdef char *strings_outname_c = strings_outname

    _hashes_from_fd(infile, hashes_tempname, strings_tempname)

    _bin = np.memmap(hashes_tempname, dtype=np.uint32).reshape((-1, 2))
    cdef np.ndarray[np.uint32_t, ndim=1] raw_hashes = _bin[:, 0]
    cdef np.ndarray[np.uint32_t, ndim=1] raw_inp_offsets = _bin[:, 1]

    _uniq_hashes, _uniq_offsets = sort_uniqify_hashes(raw_hashes, raw_inp_offsets)

    cdef np.ndarray[np.uint32_t, ndim=1] uniq_hashes = _uniq_hashes
    cdef np.ndarray[np.uint32_t, ndim=1] uniq_offsets = _uniq_offsets
    cdef np.ndarray[np.uint32_t, ndim=1] outp_offsets = np.zeros(_uniq_hashes.shape[0], dtype=np.uint32)
    cdef uint32_t n_uniq = _uniq_hashes.shape[0]


    cdef FILE *strings_outfile = fopen(strings_outname_c, "w")

    strings_temp_pyfile = open(strings_tempname, 'a+')
    cdef int strings_temp_fd = strings_temp_pyfile.fileno()
    cdef size_t strings_temp_size = os.fstat(strings_temp_fd).st_size
    cdef char *strings_tempfile = <char *> mmap(
            NULL, strings_temp_size, PROT_READ, MAP_PRIVATE, strings_temp_fd, 0)

    if (<int> strings_tempfile) == MAP_FAILED:
        raise IOError(strerror(errno))

    cdef uint32_t current_uniq_offset
    cdef size_t current_offset = 0 # offset into the output strings file


    cdef uint32_t line_size = 0
    try:
        print n_uniq
        for current_idx in range(n_uniq):

            current_uniq_offset = uniq_offsets[current_idx]

            #print current_idx, current_uniq_offset

            if current_idx % 10000 == 0:
                print current_idx

            assert current_uniq_offset < strings_temp_size

            line_size = (<uint32_t *> (&strings_tempfile[current_uniq_offset]))[0]


            #print line_size

            #if line_size > 10000:
            #    break

            current_offset = ftell(strings_outfile)
            if line_size > 1000000:
                print line_size, current_uniq_offset
                continue
            fwrite(&strings_tempfile[current_uniq_offset + sizeof(line_size)], line_size, 1, strings_outfile)
            outp_offsets[current_idx] = current_offset

    finally:
        munmap(strings_tempfile, strings_temp_size)
        strings_temp_pyfile.close()
        fclose(strings_outfile)

    np.concatenate((uniq_hashes, outp_offsets)).tofile(hashes_outname)


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
        self.hashes_length = self.hashes_file.shape[0] / 2
        self.hashes = self.hashes_file[:self.hashes_length]
        self.offsets = self.hashes_file[self.hashes_length:]
        self.strings = open(strings_fname, 'r')

    def decode(self, row):
        return row.decode('utf-8')

    def get(self, k, default=None):
        hash_idx = np.searchsorted(self.hashes, k)
        if self.hashes[hash_idx] != k:
            return default
        offset = self.offsets[hash_idx]
        self.strings.seek(offset)
        length = self.offsets[hash_idx + 1] - offset
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
