from libc.stdint cimport *
from libc.stdlib cimport malloc, free, realloc
from libc.stdio cimport FILE, fdopen, fclose, getline
from cython cimport view

from utils cimport expand_tokens, hash_bytes, tab_col_split_point
from utils cimport hash_tokens as c_hash_tokens

import numpy as np
cimport numpy as np

cdef extern from "Python.h":

    object PyString_FromStringAndSize(char *s, Py_ssize_t len)
    Py_ssize_t PyString_Size(object v)
    Py_ssize_t PyList_Size(object v)
    int PyList_Append(object l, object v)
    object PyTuple_New(Py_ssize_t size)
    int PyTuple_SetItem(object p, Py_ssize_t idx, object o)

def clean_tokens(instring):
    cdef bytes py_bytes = instring
    cdef char *c_bytes = py_bytes
    cdef size_t inp_size = len(py_bytes)
    cdef size_t max_res_size = inp_size * 2
    cdef char *res_buffer = <char *> malloc(max_res_size)
    cdef size_t res_size

    res_size = expand_tokens(c_bytes, inp_size, res_buffer, max_res_size)
    res = PyString_FromStringAndSize(res_buffer, res_size)
    free(res_buffer)
    return res

def clean_token_list(rows):
    cdef Py_ssize_t n_rows = PyList_Size(rows)
    cdef object res = PyTuple_New(n_rows)

    cdef char *c_bytes
    cdef char *res_buffer = <char *> malloc(4096)
    cdef size_t res_buffer_size = 4096
    cdef object row
    cdef size_t row_size
    cdef size_t expanded_size

    try:
        for idx in range(n_rows):
            row = rows[idx]
            c_bytes = row
            row_size = PyString_Size(row)
            if row_size * 2 > res_buffer_size:
                res_buffer = <char *> realloc(res_buffer, row_size * 2)
                res_buffer_size = row_size * 2
            expanded_size = expand_tokens(
                    c_bytes, row_size, res_buffer, res_buffer_size)
            PyTuple_SetItem(res, idx, PyString_FromStringAndSize(res_buffer, expanded_size))
    finally:
        free(res_buffer)

def hash_tokens(instring):
    cdef bytes py_bytes = instring
    cdef char *c_bytes = py_bytes
    cdef size_t inp_size = len(py_bytes)

    if inp_size < 1:
        return [[], []]

    cdef np.ndarray[np.uint64_t, ndim=1] res_buf = np.zeros((inp_size,), dtype=np.uint64)
    cdef np.ndarray[np.uint64_t, ndim=1] res_offsets = np.zeros((inp_size,), dtype=np.uint64)
    cdef np.ndarray[np.uint64_t, ndim=1] res_lengths = np.zeros((inp_size,), dtype=np.uint64)


    cdef size_t res_size = c_hash_tokens(
            c_bytes, inp_size,
            &res_buf[0], <size_t *> &res_offsets[0], <size_t *> &res_lengths[0],
            inp_size)

    return (res_buf[:res_size], np.transpose((res_offsets[:res_size], res_lengths[:res_size])))

def contains_sorted(np.ndarray sorted_arr, np.ndarray inp_arr):
    cdef size_t inp_size = inp_arr.shape[0]
    cdef size_t sorted_size = sorted_arr.shape[0]
    cdef np.ndarray res = np.zeros((inp_size,), dtype=np.bool)
    cdef size_t left
    cdef size_t right
    cdef size_t pivot_idx
    cdef int pivot
    cdef int target

    for idx in range(inp_size):
        target = inp_arr[idx]
        left = 0
        right = sorted_size
        while right > left:
            pivot_idx = left + (right - left) / 2
            pivot = sorted_arr[pivot_idx]
            if pivot == target:
                res[idx] = 1
                break
            elif pivot > target:
                left = pivot_idx
            else:
                right = pivot_idx

    return res


def read_tab_groups(infobj, tab_split_point):
    "returns squence of (key, key_hash, values)"

    cdef char *line_buf = <char *> malloc(1024)
    cdef size_t line_buf_size = 1024

    cdef FILE *inf
    cdef uint64_t prev_key_hash = 0
    cdef uint64_t cur_key_hash
    cdef object current_key
    cdef object current_val
    cdef object current_val_list = []
    cdef ssize_t current_line_size
    cdef size_t current_split_point

    try:
        inf = fdopen(infobj.fileno(), "r")
        try:
            while 1:
                current_line_size = getline(&line_buf, &line_buf_size, inf)
                if current_line_size < 0:
                    break
                if current_line_size < 1:
                    continue
                current_split_point = tab_col_split_point(
                        line_buf, current_line_size, 2)
                cur_key_hash = hash_bytes(
                        &line_buf[current_split_point],
                        current_line_size - current_split_point)

                current_val = PyString_FromStringAndSize(
                        &line_buf[current_split_point],
                        current_line_size - current_split_point)

                if cur_key_hash != prev_key_hash:
                    if PyList_Size(current_val_list) > 0:
                        yield (current_key, prev_key_hash, current_val_list)
                    current_val_list = []
                    current_key = PyString_FromStringAndSize(
                            line_buf, current_split_point)

                PyList_Append(current_val_list, current_val)

            if PyList_Size(current_val_list) > 0:
                yield (current_key, prev_key_hash, current_val_list)
                current_val_list = []

        finally:
            fclose(inf)
    finally:
        free(line_buf)
