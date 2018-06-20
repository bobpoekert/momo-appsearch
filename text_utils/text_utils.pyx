from libc.stdint cimport *
from libc.stdlib cimport malloc, free

cdef extern from "clean_tokens.h":

    size_t expand_tokens(char *inp, size_t inp_size, char *out_buf, size_t max_outp_size);

cdef extern from "Python.h":

    object PyString_FromStringAndSize(char *s, Py_ssize_t len)

def clean_tokens(instring):
    cdef bytes py_bytes = instring.encode('utf-8')
    cdef char *c_bytes = py_bytes
    cdef size_t inp_size = len(py_bytes)
    cdef size_t max_res_size = inp_size * 2
    cdef char *res_buffer = <char *> malloc(max_res_size)
    cdef size_t res_size

    res_size = expand_tokens(c_bytes, inp_size, res_buffer, max_res_size)
    res = PyString_FromStringAndSize(res_buffer, res_size)
    free(res_buffer)
    return res
