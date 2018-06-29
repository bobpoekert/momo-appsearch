#!/usr/bin/env python2.7

import numpy as np
import sys
from variances import variances, write_translation_index

if __name__ == '__main__':
    hashes = np.memmap(sys.argv[1], dtype=np.uint64).reshape((-1, 3))

    keys, vs = variances(hashes)

    with open(sys.argv[2], 'w') as outf:
        keys.tofile(outf)
        vs.tofile(outf)

    with open(sys.argv[3], "w") as idx_outf:
        with open(sys.argv[4], 'w') as outf:
            write_translation_index(hashes, outf, idx_outf)
