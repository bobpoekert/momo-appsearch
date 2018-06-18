#!/usr/bin/env python2.7

import numpy as np
import sys
from variances import variances
sys.path.append('../sstable')
from sstable import build_mat_index

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

    build_mat_index(a_sorted, np.transpose((b_sorted, scores_e)), sys.argv[2], '%s.txt' % sys.argv[2])

    build_mat_index(a_sorted, amb_e, sys.argv[3], '%s.txt' % sys.argv[3])
