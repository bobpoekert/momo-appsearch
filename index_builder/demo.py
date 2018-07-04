import sys, os
import numpy as np

sys.path.append('../sstable')
sys.path.append('../text_utils')

import sstable
import text_utils

import sent2vec
from annoy import AnnoyIndex

from collections import namedtuple

class DoubleIndex(object):


    def __init__(self, fname):
        self.fsize = os.stat(fname).st_size
        self.file = open(fname, 'a+')
        self.n_keys = self.fsize / 8 / 2
        self.hashes = np.memmap(self.file, dtype=np.uint64, offset=0)
        self.vals = np.memmap(self.file, dtype=np.float64, offset=self.n_keys * 8)

    def get(self, k, default=None):
        idx = sstable.searchsorted_uint64(self.hashes, k)
        if idx is None:
            return default
        return self.vals[idx]

class TranslationIndex(sstable.SSTable):

    def decode(self, v):
        return np.frombuffer(v, dtype=np.uint64).reshape((-1, 2))

class CleanHashesIndex(sstable.SSTable):

    def decode(self, v):
        return np.frombuffer(v, dtype=np.uint64)

SuggestResult = namedtuple('SuggestResult', 'key string raw_strings translations ambiguity')
Translation = namedtuple('Translation', 'locale string raw_strings score')

class Backend(object):

    def __init__(self):
        self._vec_model = None
        self._annoy_index = None

        self.annoy_indexes = np.memmap('english.annoy.indexes', dtype=np.uint64)
        self.clean_hash_mat = np.memmap('clean_hashes.bin', dtype=np.uint64)
        self.n_hashes = self.clean_hash_mat.shape[0] / 2
        self.clean_hashes = self.clean_hash_mat[:self.n_hashes]
        self.raw_hashes = self.clean_hash_mat[self.n_hashes:]
        self.ambiguities = DoubleIndex('ambiguity.bin')
        self.translations = TranslationIndex('translations.sstable', 'translations.txt.sstable')
        self.clean_strings = sstable.SSTable('/mnt/zapk/strings.sstable', '/mnt/zapk/strings.txt.sstable')
        self.raw_strings = sstable.SSTable('/mnt/zapk/raw_strings.sstable', '/mnt/zapk/raw_strings.txt.sstable')

    @property
    def vec_model(self):
        if self._vec_model is None:
            print 'loading vec model'
            self._vec_model = sent2vec.Sent2vecModel()
            self._vec_model.load_model('/mnt/zapk/english.sent2vec.bin')
        return self._vec_model

    @property
    def annoy_index(self):
        if self._annoy_index is None:
            print 'loading ann index'
            self._annoy_index = AnnoyIndex(self.vec_model.get_emb_size(), metric='angular')
            self._annoy_index.load('english.annoy')
        return self._annoy_index

    def clean_hash_from_ann_index(self, idx):
        return self.clean_strings.hashes[self.annoy_indexes[idx]]

    def clean_text(self, v):
        return text_utils.clean_tokens(v.encode('utf-8'))

    def sent2vec_neighbors(self, v, k=100):
        v = self.clean_text(v)
        target_vec = self.vec_model.embed_sentence(v)
        neighbor_ids, distances = self.annoy_index.get_nns_by_vector(target_vec, k, include_distances=True)
        neighbor_idxes = self.annoy_indexes[neighbor_ids]
        neighbor_hashes = self.clean_strings.hashes[neighbor_idxes]
        return (neighbor_hashes, distances)

    def get_clean_string(self, h):
        return self.clean_strings.get(h)

    def get_raw_hash_from_clean(self, h):
        idx = sstable.searchsorted_uint64(self.clean_hashes, h)
        return self.raw_hashes[idx]

    def get_raw_strings(self, h):
        "clean_string hash -> raw strings"
        return self.raw_strings.get(self.get_raw_hash_from_clean(h))

    def get_ambiguity(self, k):
        return self.ambiguities.get(k)

    def get_translations(self, k):
        hashes = self.translations.get(k)
        if hashes is None:
            return []
        n_hashes = hashes.shape[0]
        total_score = np.sum(hashes[:, 1])
        normed_scores = hashes[:, 1] / total_score
        res = []
        for idx in range(n_hashes):
            h = hashes[idx, 0]
            score = normed_scores[idx]
            clean_string = self.get_clean_string(h)
            raw_strings = self.get_raw_strings(h)
            res.append(Translation(locale, clean_string, raw_strings, score))
        res.sort(key=lambda v: v.score, reverse=True)
        return res

    def suggest(self, v, k=10):
        ks, distances = self.sent2vec_neighbors(v, k=(k*10))
        ambiguities = dict((k, self.get_ambiguity(k)) for k in ks)
        ks = sorted(ks, key=lambda v: ambiguities[v])

        top_ks = ks[:k]
        res = []
        for k in top_ks:
            clean_string = self.get_clean_string(k)
            raw_strings = self.get_raw_strings(k)
            translations = self.get_translations(k)
            res.append(SuggestResult(k, clean_string, raw_strings, translations, ambiguities[k]))
        return res

_backend = None
def backend(vec_model=None, ann_index=None):
    global _backend
    if _backend is None:
        _backend = Backend()
        if vec_model is not None:
            _backend._vec_model = vec_model
        if ann_index is not None:
            _backend._annoy_index = ann_index
    return _backend

