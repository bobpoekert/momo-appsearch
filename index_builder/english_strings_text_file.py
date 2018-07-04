import sys, traceback

sys.path.append('../sstable')
import sstable

sys.path.append('../text_utils')
import text_utils as tt

import numpy as np

latin_letters= {}


def only_roman_chars(unistr):
    for c in unistr:
        if (ord(c) & 0xC0) == 0x80:
            return False
    return True

if __name__ == '__main__':

    english_hashes = np.memmap(sys.argv[4], dtype=np.uint64)
    english_hashes_idx = 0
    max_english_hashes = english_hashes.shape[0]
    table = sstable.SSTable(sys.argv[1], sys.argv[2])
    with open(sys.argv[3], 'w') as outf:
        for idx in xrange(table.hashes.shape[0]):
            if english_hashes_idx >= max_english_hashes:
                break
            if english_hashes[english_hashes_idx] == table.hashes[idx]:
                v = table.read_idx(idx)
                if only_roman_chars(v):
                    outf.write('%s\n' % v)
                english_hashes_idx += 1
            while english_hashes_idx < max_english_hashes and \
                    english_hashes[english_hashes_idx] < table.hashes[idx]:
                english_hashes_idx += 1
