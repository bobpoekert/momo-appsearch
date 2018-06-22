
from annoy import AnnoyIndex
import numpy as np
import sys

sys.path.append('../sstable')
import sstable

vecs_txt_fname = sys.argv[1]
hashes_bin_fname = sys.argv[2]
annoy_index_fname = sys.argv[3]

hashes_bin = np.memmap(hashes_bin_fname, dtype=np.uint64)
n_hashes = hashes_bin.shape[0] / 2
hashes = hashes_bin[:n_hashes]
offsets = hashes_bin[n_hashes:]

annoy_index = AnnoyIndex(300, metric='angular')
inf = open(vecs_txt_fname, 'r')
next(inf) # first line is garbage

print 'loading'
for idx, row in enumerate(inf):
    row = row.strip()
    parts = row.split(' ')
    word = parts[0]
    vec = map(float, parts[1:])
    idx = sstable.searchsorted_uint64(hashes, sstable.hash_string(word))
    annoy_index.add_item(idx, vec)

print 'building'
annoy_index.build(10)

print 'saving'
annoy_index.save(annoy_index_fname)
