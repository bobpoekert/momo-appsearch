import numpy as np

def variances(int n_uniq, unsigned int[:] keys, double[:] counts):
    cdef double[:] res = np.zeros(n_uniq)
    cdef unsigned long [:] res_counts = np.zeros(n_uniq, dtype=np.uint64)
    cdef double [:] sums = np.zeros(n_uniq, dtype=np.float64)
    cdef double [:] sums_squares = np.zeros(n_uniq, dtype=np.float64)
    cdef double [:] translation_probs = np.zeros(keys.shape[0], dtype=np.float64)

    cdef unsigned int res_idx = 0
    cdef unsigned int count = 1
    cdef double _sum = counts[0]
    cdef double sum_squares = _sum * _sum
    cdef unsigned int prev_k = keys[0]
    cdef unsigned int k
    cdef double v
    cdef unsigned int i = 1
    cdef unsigned int j = 0
    cdef unsigned int max_i = keys.shape[0]
    cdef double mean

    cdef unsigned int translation_start_idx = 0
    cdef unsigned int translation_probs_idx = 0

    while i < max_i:
        k = keys[i]
        v = counts[i]
        if k == prev_k:
            _sum += v
            sum_squares += v * v
            count += 1
        else:
            mean = _sum / count
            res[res_idx] = (sum_squares / count) - (mean * mean)
            res_counts[res_idx] = count
            sums[res_idx] = _sum
            sums_squares[res_idx] = _sum * _sum

            j = translation_start_idx
            while j < i:
                if _sum > 0:
                    translation_probs[j] = counts[j] / _sum
                j += 1

            res_idx += 1
            count = 1
            _sum = v
            sum_squares = v * v
            prev_k = k
            translation_start_idx = i
        i += 1
    mean = _sum / <double> count
    res[res_idx] = (sum_squares / <double> count) - (mean * mean)
    res_counts[res_idx] = count
    sums[res_idx] = _sum
    sums_squares[res_idx] = _sum * _sum
    return np.array(res), np.array(res_counts), np.array(sums), np.array(sums_squares), np.array(translation_probs)
