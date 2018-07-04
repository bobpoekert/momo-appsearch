import numpy as np
cimport numpy as np

from libc.stdio cimport fdopen, fclose, FILE, fwrite, fflush
from libc.stdint cimport *

def variances(_mat):
    cdef np.ndarray[np.uint64_t, ndim=2] mat = _mat
    cdef uint64_t mat_size = _mat.shape[0]
    cdef np.ndarray[np.uint64_t, ndim=1] keys = np.zeros((mat_size,), dtype=np.uint64)
    cdef np.ndarray[np.float64_t, ndim=1] vals = np.zeros((mat_size,), dtype=np.float64)

    cdef uint64_t prev_hash = 0
    cdef uint64_t cur_hash
    cdef uint64_t res_idx = 0
    cdef uint64_t idx = 0

    cdef uint64_t cur_val
    cdef uint64_t cur_sum = 0
    cdef uint64_t cur_sum_squares = 0
    cdef uint64_t cur_count = 0
    cdef double cur_mean

    prev_hash = mat[0, 0]
    cur_sum = mat[0, 1]
    cur_sum_squares = cur_sum * cur_sum
    cur_count = 1
    idx = 1

    while idx < mat_size:
        cur_hash = mat[idx, 0]
        cur_val = mat[idx, 2]
        if cur_hash != prev_hash:
            cur_mean = cur_sum / cur_count
            vals[res_idx] = (cur_sum_squares / cur_count) - (cur_mean * cur_mean)
            keys[res_idx] = prev_hash
            res_idx += 1
            cur_sum = 0
            cur_sum_squares = 0
            cur_count = 0
            prev_hash = cur_hash

        cur_sum += cur_val
        cur_sum_squares += cur_val * cur_val
        cur_count += 1

        idx += 1
        if idx % 10000 == 0:
            print idx


    cur_mean = cur_sum / cur_count
    vals[res_idx] = (cur_sum_squares / cur_count) - (cur_mean * cur_mean)
    keys[res_idx] = prev_hash

    return (keys[:res_idx], vals[:res_idx])

def write_translation_index(_mat, _outf, _idx_outf):
    cdef np.ndarray[np.uint64_t, ndim=2] mat = _mat
    cdef uint64_t mat_size = _mat.shape[0]
    cdef np.ndarray[uint64_t, ndim=1] offsets = np.zeros((mat_size,), dtype=np.uint64)
    cdef uint64_t idx = 0
    cdef uint64_t res_idx = 1
    cdef uint64_t byte_offset = 0

    cdef FILE *outf = fdopen(_outf.fileno(), "w")
    cdef uint64_t outbuf[2]

    cdef uint64_t cur_key
    cdef uint64_t prev_key

    try:
        cur_key = mat[0, 0]

        while idx < mat_size:
            cur_key = mat[idx, 0]
            if cur_key != prev_key:
                offsets[res_idx] = byte_offset
                res_idx += 1

            outbuf[0] = mat[idx, 1]
            outbuf[1] = mat[idx, 2]
            fwrite(&outbuf[0], sizeof(uint64_t), 3, outf)
            byte_offset += sizeof(uint64_t) * 3

            idx += 1

        mat[:, 0].tofile(_idx_outf)
        offsets.tofile(_idx_outf)
    finally:
        fflush(outf)
