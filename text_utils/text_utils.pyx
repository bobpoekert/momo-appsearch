from libc.stdint cimport *
from libc.stdlib cimport malloc, free
from cython cimport view

from utils cimport expand_tokens
from utils cimport hash_tokens as c_hash_tokens

cdef extern from "Python.h":

    object PyString_FromStringAndSize(char *s, Py_ssize_t len)

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

def hash_tokens(instring):
    cdef bytes py_bytes = instring
    cdef char *c_bytes = py_bytes
    cdef size_t inp_size = len(py_bytes)

    if inp_size < 1:
        return []

    cdef uint64_t[:] res_buf = view.array(shape=(inp_size,), itemsize=sizeof(uint64_t), format='Q')

    cdef size_t res_size = c_hash_tokens(c_bytes, inp_size, &res_buf[0], inp_size)

    return res_buf[:res_size]
