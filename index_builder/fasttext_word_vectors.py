
from annoy import AnnoyIndex
import numpy as np
import sys

sys.path.append('../sstable')
import sstable

vecs_txt_fname = sys.argv[1]
hashes_bin_fname = sys.argv[2]
hashes_txt_fname = sys.argv[3]

hashes = []
vectors = []

infile = open(vecs_txt_fname, 'r')
next(infile) # first row is bogus

print 'loading'
for row in infile:
    cols = row.split(' ')
    k = cols[0]
    v = np.array(map(float, cols[1:]), dtype=np.float32)
    h = sstable.hash_string(k)
    hashes.append(h)
    vectors.append(v)

print 'sorting'
hashes = np.array(hashes, dtype=np.uint64)
vectors = np.array(vectors, dtype=np.float32)
sort_indexes = np.argsort(hashes)

vectors = vectors[sort_indexes]
hashes = hashes[sort_indexes]

row_size = vectors.shape[1] * np.dtype(np.float32).itemsize
offsets = np.arange(0, vectors.shape[0] * row_size, row_size, dtype=np.uint64)

print hashes.shape, offsets.shape
assert hashes.shape == offsets.shape

print 'saving'
np.concatenate([hashes, offsets]).astype(np.uint64).tofile(hashes_bin_fname)
vectors.tofile(hashes_txt_fname)
