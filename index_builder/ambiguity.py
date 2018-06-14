#!/usr/bin/env python2.7

import numpy as np
import sys
from variances import variances

if __name__ == '__main__':
    hashes = np.memmap(sys.argv[1], dtype=[('a', np.uint32), ('b', np.uint32), ('count', np.uint64)])

    a_ids = np.argsort(hashes['a'])
    a_sorted = hashes['a'][a_ids]
    b_sorted = hashes['b'][a_ids]

    uniq, uniq_inverse = np.unique(a_sorted, return_inverse=True)

    count_sorted = hashes['count'].astype(np.uint32)[a_ids]

    amb_t, counts_t, sums_t, sums_squares_t, translation_probs = variances(uniq.shape[0], a_sorted, count_sorted.astype(np.float64))

    scores_e = amb_t[uniq_inverse].astype(np.float64) * translation_probs
    scores_e[scores_e <= 0] = 1
    scores_e = np.log(scores_e)
    scores_e[scores_e < 0] = 0
    scores_e[scores_e > 10] = 10

    amb_e, counts_e, sums_e, sums_squares_e, _ = variances(uniq.shape[0], a_sorted, scores_e)

    with open(sys.argv[2], 'w') as outf:
        # chunk_size = file_size / 4
        # a_start = 0
        # b_start = chunk_size
        # scores_start = chunk_size * 2
        # n_rows = chunk_size / 4
        a_sorted.tofile(outf)
        b_sorted.tofile(outf)
        scores_e.tofile(outf)

    with open(sys.argv[3], 'w') as outf:
        # amb_start = file_size / 3
        a_sorted.tofile(outf)
        amb_t.tofile(outf)
