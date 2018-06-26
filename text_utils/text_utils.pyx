from libc.stdint cimport *
from libc.stdlib cimport malloc, free, realloc
from libc.stdio cimport FILE, fdopen, fclose, getline
from libc.string cimport memcpy
from cython cimport view

from utils cimport expand_tokens, hash_bytes, tab_col_split_point
from utils cimport hash_tokens as c_hash_tokens

import numpy as np
cimport numpy as np

import sys

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

def clean_token_list(rows, colid):
    res = []
    for row in rows:
        cols = row.split('\t', colid)
        k = '\t'.join(cols[:-1])
        v = cols[-1]

        res.append('%s\t%s' % (k, clean_tokens(v)))

    return res

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

cdef searchsorted_uint64(np.ndarray[uint64_t, ndim=1] inp, uint64_t target):
    cdef uint64_t size = inp.shape[0]
    cdef uint64_t right = size
    cdef uint64_t left = 0
    cdef uint64_t pivot
    cdef uint64_t pivot_idx

    if target < inp[0] or target > inp[size - 1]:
        return None

    while right > left:
        pivot_idx = left + (right - left) / 2
        pivot = inp[pivot_idx]
        if pivot == target:
            return pivot_idx
        elif pivot > target:
            right = pivot_idx
        elif pivot < target:
            left = pivot_idx

    return None

def contains_sorted(_sorted_arr, _inp_arr):
    cdef np.ndarray sorted_arr = _sorted_arr
    cdef np.ndarray inp_arr = _inp_arr
    cdef size_t inp_size = inp_arr.shape[0]
    cdef size_t sorted_size = sorted_arr.shape[0]
    cdef np.ndarray res = np.zeros((inp_size,), dtype=np.bool)

    for i in range(inp_size):
        idx = np.searchsorted(sorted_arr, inp_arr[i])
        if idx < sorted_size and sorted_arr[idx] == inp_arr[i]:
            res[i] = 1

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

def substitute_propernames(vals):

    res = []
    cleaned_vals = clean_token_list(vals, 1)

    if len(cleaned_vals) == 0:
        return res

    if len(cleaned_vals) < 2:
        res.append(cleaned_vals[0])
        return res

    token_hashes_and_offsets = map(hash_tokens, cleaned_vals)

    done = False
    for k in token_hashes_and_offsets:
        if k is None:
            res.extend(cleaned_vals)
            return res

    token_hashes = [np.array(v[0]) for v in token_hashes_and_offsets]
    token_offsets = [v[1] for v in token_hashes_and_offsets]

    all_row_hashes = reduce(np.intersect1d, token_hashes)

    if len(all_row_hashes) < 1:
        res.extend(cleaned_vals)
    elif len(all_row_hashes) == len(token_hashes):
        # virbatim copy in all languages, not useful for parallel string corpus
        return res
    else:
        propername_hashes = all_row_hashes
        propername_hashes = np.sort(propername_hashes)

        propername_masks = [contains_sorted(propername_hashes, v) for v in token_hashes]
        propername_offsets = [a[b] for a, b in zip(token_offsets, propername_masks)]
        replaced_hashes = [a[b] for a, b in zip(token_hashes, propername_masks)]

        replacers = ['$$propername%d$$' % i for i in range(propername_hashes.shape[0])]

        for idx, v in enumerate(cleaned_vals):
            offset_delta = 0
            for (start, length), h in zip(propername_offsets[idx], replaced_hashes[idx]):
                if length > 1:
                    start = int(start)
                    length = int(length)
                    replacer = replacers[np.searchsorted(propername_hashes, h)]
                    start += offset_delta
                    v = v[:start] + replacer + v[(start + length):]
                    offset_delta += (len(replacer) - length)
            res.append(v)

    return res
