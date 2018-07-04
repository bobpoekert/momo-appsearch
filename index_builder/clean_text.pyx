import sys
sys.path.append('../sstable')
import sstable
sys.path.append('../text_utils')
import text_utils

import numpy as np

dictionary = sstable.SSTable(sys.argv[1], sys.argv[2])

import struct

def read_tab_groups(inf):
    #return text_utils.read_tab_groups(inf, 2)
    current_key = None
    current_val_list = []
    for row in inf:
        if not row.strip():
            continue
        parts = row[:-1].split('\t', 3)
        try:
            k = '\t'.join(parts[:-2])
            v = (parts[-2], parts[-1])
        except:
            print parts
            continue
        if k == current_key:
            current_val_list.append(v)
        else:
            yield (current_key, current_val_list)
            current_key = k
            current_val_list = [v]

    yield (current_key, current_val_list)

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
        hashes, offsets = text_utils.hash_tokens(v)
    except ValueError:
        return None
    hashes = np.array(hashes)
    mask = offsets[:, 1] > 0
    hashes = hashes[mask]
    offsets = offsets[mask]
    return (hashes, offsets)

def run(inf, outf, cleaned_hashes_table, english_hashes_table, max_bytes):
    for key, vals in read_tab_groups(inf):
        stripped_vals = [v[1] for v in vals]
        cleaned_vals = [v.strip() for v in text_utils.substitute_propernames(stripped_vals)]

        for v in cleaned_vals:
            outf.write('%s\t%s\n' % (key, v))

        for a, b in zip(cleaned_vals, vals):
            ah = sstable.hash_string(a)
            cleaned_hashes_table.write(struct.pack('QQ',
                ah, sstable.hash_string('%s\t%s' % (b[0], b[1]))))
            locale = b[0].strip()
            if len(locale) < 1 or locale.startswith('-en'):
                english_hashes_table.write(struct.pack('Q', ah))

        if key is not None:
            max_bytes -= sum(map(len, vals))
            max_bytes -= len(key) * len(vals)
            max_bytes -= len(vals) * 2
            if max_bytes < 1:
                break
