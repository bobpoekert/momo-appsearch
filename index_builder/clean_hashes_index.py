import numpy as np
import sys

sys.path.append('../sstable')
import sstable

if __name__ == '__main__':
    inf = np.memmap(sys.argv[1], dtype=np.uint64).reshape((-1,2))
    sort_indexes = np.argsort(inf[:, 0])

    with open(sys.argv[2], 'w') as boutf:
        with open(sys.argv[3], 'w') as toutf:
            sstable.build_mat_index(
                    inf[sort_indexes, 0],
                    inf[sort_indexes, 1],
                    boutf, toutf)
