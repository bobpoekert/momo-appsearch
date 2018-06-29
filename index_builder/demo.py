import sys
import numpy as np

sys.path.append('../sstable')
sys.path.append('../text_utils')

import sstable
import text_utils

import sent2vec
from annoy import AnnoyIndex

class Backend(object):

    def __init__(self):
        print 'loading vec model'
        self.vec_model = sent2vec.Sent2vecModel()
        self.vec_model.load_model('/mnt/zapk/english.sent2vec.bin')

        print 'loading ann index'
        self.annoy_index = AnnoyIndex(vec_model.get_emb_size(), metric='angular')
        self.annoy_index.load('english.annoy')

        self.annoy_indexes = np.memmap('english.annoy.indexes', dtype=np.uint64)

        self.clean_indexes = sstable.SSTable('clean_hashes.sstable', 'clean_hashes.txt.sstable') 

    def clean_text(self, v):
        return text_utils.clean_tokens(v.encode('utf-8'))

    def sent2vec_neighbors(self, v, k=100):
        v = self.clean_text(v)
        target_vec = self.vec_model.embed_sentence(v)
        neighbors, distances = self.annoy_index.get_nns_by_vector(target_vec, k, include_distances=True)
        return (self.annoy_indexes[neighbors], distances)

    def get_clean_string(self, h):
        return self.clean_strings.get(h)

    def get_raw_strings(self, h):
        raw_clean_indexes = self.clean_indexes.get(h)
        clean_indexes = np.frombuffer(raw_clean_indexes, dtype=np.uint64)
        return [self.raw_strings.read_idx(v) for v in clean_indexes]
