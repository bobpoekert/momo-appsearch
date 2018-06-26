import sys
sys.path.append('../sstable')
import sstable
sys.path.append('../text_utils')
import text_utils

import numpy as np

dictionary = sstable.SSTable(sys.argv[1], sys.argv[2])

import struct

def read_tab_groups(inf):
    return text_utils.read_tab_groups(inf, 2)
    '''
    current_key = None
    current_val_list = []
    for row in inf:
        parts = row.strip().split('\t', 2)
        k = '\t'.join(parts[:-1])
        v = parts[-1]
        if k == current_key:
            current_val_list.append(v)
        else:
            yield (current_key, current_val_list)
            current_key = k
            current_val_list = [v]

    yield (current_key, current_val_list)
    '''

def clean_token_list(rows, colid):
    res = []
    for row in rows:
        cols = row.split('\t', colid)
        k = '\t'.join(cols[:-1])
        res.append('%s\t%s' % (k, text_utils.clean_tokens(cols[-1])))
    return res

def contains_sorted(sorted_arr, target):
    res = np.zeros(target.shape, dtype=np.bool)

    for idx in range(target.shape[0]):
        v = target[idx]
        v_idx = sstable.searchsorted_uint64(sorted_arr, v)
        if v_idx is not None and sorted_arr[v_idx] == v:
            res[idx] = 1

    return res

def hash_tokens(v):
    try:
        k, v2 = v.split('\t', 1)
    except ValueError:
        v2 = v
        k = ''
    try:
        hashes, offsets = text_utils.hash_tokens(v2)
    except ValueError:
        return None
    hashes = np.array(hashes)
    mask = offsets[:, 1] > 0
    hashes = hashes[mask]
    offsets = offsets[mask]
    offsets[:, 0] += len(k) + 1
    return (hashes, offsets)

def run():
    with open(sys.argv[3], 'w') as cleaned_hashes_table:
        for key, key_hash, vals in read_tab_groups(sys.stdin):

            cleaned_vals = clean_token_list(vals, 1)

            if len(cleaned_vals) == 0:
                continue

            if len(cleaned_vals) < 2:
                print '%s\t%s' % (key, cleaned_vals[0])
                continue

            token_hashes_and_offsets = map(hash_tokens, cleaned_vals)

            done = False
            for k in token_hashes_and_offsets:
                if k is None:
                    for v in cleaned_vals:
                        print '%s\t%s' % (key, v)
                    done = True
                    break
            if done == True:
                continue

            token_hashes = [np.array(v[0]) for v in token_hashes_and_offsets]
            token_offsets = [v[1] for v in token_hashes_and_offsets]

            all_row_hashes = reduce(np.intersect1d, token_hashes)

            if len(all_row_hashes) < 1:
                for v in cleaned_vals:
                    print '%s\t%s' % (key, v)
            elif len(all_row_hashes) == len(token_hashes):
                # virbatim copy in all languages, not useful for parallel string corpus
                continue
            else:
                #propername_hashes = all_row_hashes[dictionary.getall(all_row_hashes) < 1]
                propername_hashes = all_row_hashes
                propername_hashes = np.sort(propername_hashes)

                propername_masks = [contains_sorted(propername_hashes, v) for v in token_hashes]
                propername_offsets = [a[b] for a, b in zip(token_offsets, propername_masks)]
                replaced_hashes = [a[b] for a, b in zip(token_hashes, propername_masks)]

                replacers = ['$$propername%d$$' % i for i in range(propername_hashes.shape[0])]

                for idx, v in enumerate(cleaned_vals):
                    offset_delta = 0
                    for (start, length), h in zip(propername_offsets[idx], replaced_hashes[idx]):
                        if length > 1:
                            start = int(start)
                            length = int(length)
                            replacer = replacers[np.searchsorted(propername_hashes, h)]
                            start += offset_delta
                            v = v[:start] + replacer + v[(start + length):]
                            offset_delta += (len(replacer) - length)
                    print '%s\t%s' % (key, str(v))
            for a, b in zip(cleaned_vals, vals):
                cleaned_hashes_table.write(struct.pack('QQ', sstable.hash_string(a), sstable.hash_string(b)))
