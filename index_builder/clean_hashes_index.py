import numpy as np
import sys

if __name__ == '__main__':
    inf = np.memmap(sys.argv[1], dtype=np.uint64).reshape((-1,2))

    inf_clean = inf[:, 0]
    inf_raw = inf[:, 1]

    uniq_clean, uniq_clean_indexes = np.unique(inf_clean, return_index=True)
    uniq_raw = inf_raw[uniq_clean_indexes]

    assert np.count_nonzero(uniq_clean[1:] <= uniq_clean[:-1]) < 2

    with open(sys.argv[2], 'w') as boutf:
        np.concatenate([uniq_clean, uniq_raw]).astype(np.uint64).tofile(boutf)

    english_hashes = np.memmap(sys.argv[4], dtype=np.uint64)
    with open(sys.argv[3], 'w') as uniq_english_hashes:
        np.unique(english_hashes).tofile(uniq_english_hashes)
