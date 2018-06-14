from libc.stdint cimport uint32_t

cdef extern from "pymcdb.h":
    void *db_init(int fd)
    void db_free(void *ctx)
    char db_next(void *ctx, char **k, uint32_t *ksize, char **v, size_t *vsize)
    char db_int_key_next(void *ctx, uint32_t *k, char **v, size_t *vsize);

def db_data(inf):
    cdef void *ctx = db_init(inf.fileno())
    if ctx == NULL:
        raise RuntimeError('Failed to load db file')
    cdef size_t ksize
    cdef uint32_t k
    cdef size_t vsize
    cdef char *v

    try:
        while db_int_key_next(ctx, &k, &v, &vsize):
            yield (k, v[:vsize].decode('UTF-8'))
    finally:
        db_free(ctx)
