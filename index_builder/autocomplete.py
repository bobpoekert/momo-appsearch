import sys
import numpy as np

sys.path.append('../sstable')
sys.path.append('../text_utils')

import sstable
import text_utils

import sent2vec
from annoy import AnnoyIndex

print 'loading vec model'
vec_model = sent2vec.Sent2vecModel()
vec_model.load_model('/mnt/zapk/english.sent2vec.bin')

print 'loading ann index'
annoy_index = AnnoyIndex(vec_model.get_emb_size(), metric='angular')
annoy_index.load('english.annoy')

annoy_indexes = np.memmap('english.annoy.indexes', dtype=np.uint64)

string_table = sstable.SSTable('/mnt/zapk/strings.sstable', '/mnt/zapk/strings.txt.sstable')

def neighbors(instring, k=20):
    target_vec = vec_model.embed_sentence(text_utils.clean_tokens(instring.encode('utf-8')))
    neighbors, distances = annoy_index.get_nns_by_vector(target_vec, k, include_distances=True)
    neighbors = annoy_indexes[np.array(neighbors)]
    neighbor_strings = [string_table.read_idx(i).split('\t', 1)[1].decode('utf-8') for i in neighbors]
    return (neighbor_strings, distances)

