import sys
sys.path.append('../sstable')
import sstable
sys.path.append('../text_utils')
import text_utils

import numpy as np

dictionary = sstable.SSTable('???', '???')

import struct

with open(sys.argv[1], 'w') as cleaned_hashes_table:

    for key, key_hash, vals in text_utils.read_tab_groups(sys.stdin, 2):
        cleaned_vals = text_utils.clean_token_list(vals)
        token_hashes_and_offsets = map(sstable.hash_tokens, cleaned_vals)
        token_hashes = [v[0] for v in token_hashes_and_offsets]
        token_offsets = [v[1] for v in token_hashes_and_offsets]

        all_row_hashes = reduce(np.intersect1d, token_hashes)
        propername_hashes = all_row_hashes[dictionary.getall(all_row_hashes) == -1]
        propername_hashes.sort()

        propername_masks = [text_utils.contains_sorted(propername_hashes, v) for v in token_hashes]
        propername_offsets = [a[b] for a, b in zip(token_offsets, propername_masks)]
        replaced_hashes = [a[b] for a, b in zip(token_hashes, propername_masks)]

        replacers = ['$$propername%d$$' % i for i in range(propername_hashes.shape[0])]

        for idx, v in enumerate(cleaned_vals):
            v = bytearray(v)
            for offsets, hashes in zip(propername_offsets[idx], replaced_hashes[idx]):
                for (start, end), h in zip(offsets, hashes):
                    replacer = replacers[np.searchsorted(propername_hashes, h)]
                    v[start:end] = replacer
            print '%s\t%s' % (key, str(v))

        for v1, v2 in zip(vals, cleaned_vals):
            cleaned_hashes_table.write(struct.pack('QQ', (sstable.hash_string(v1), sstable.hash_string(v2))))
