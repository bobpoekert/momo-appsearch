#!/usr/bin/env python2.7

import sent2vec
from annoy import AnnoyIndex
import numpy as np
from itertools import izip
import sys, traceback

sys.path.append('../sstable')
import sstable

sys.path.append('../text_utils')
import text_utils as tt

import unicodedata as ud

latin_letters= {}

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr)
           for uchr in unistr
           if uchr.isalpha()) # isalpha suggested by John Machin

if __name__ == '__main__':
    import sys

    print 'loading vec model'
    vec_model = sent2vec.Sent2vecModel()
    vec_model.load_model(sys.argv[4])

    english_hashes = np.memmap(sys.argv[5], dtype=np.uint64)

    def is_english(h):
        return sstable.searchsorted_uint64(english_hashes, h) is not None

    print 'embedding strings'
    vectors = []
    indexes = []
    index_batch = []
    string_batch = []
    seen_strings = set([])
    table = sstable.SSTable(sys.argv[1], sys.argv[2])
    for idx, v in enumerate(table.raw_itervalues()):
        try:
            if not is_english(table.hashes[idx]):
                continue
            if len(v) < 1:
                continue
            if not only_roman_chars(v.decode('utf-8')):
                continue
            if v in seen_strings:
                continue
            seen_strings.add(v)
            if idx % 10000 == 0:
                print idx, v
            try:
                string_batch.append(v.decode('utf-8'))
                index_batch.append(idx)
            except:
                print repr(v)
                traceback.print_exc()

            if len(string_batch) > 10000:
                vec_batch = vec_model.embed_sentences(string_batch)
                nonzeros = np.count_nonzero(vec_batch, axis=1) > 0
                vectors.append(vec_batch[nonzeros, :])
                for i, v in enumerate(index_batch):
                    if nonzeros[i]:
                        indexes.append(v)
                index_batch[:] = []
                string_batch[:] = []
                print idx, len(indexes)
        except:
            traceback.print_exc()

    try:
        if len(index_batch) > 0:
            vec_batch = vec_model.embed_sentences(string_batch)
            nonzeros = np.count_nonzero(vec_batch, axis=2) > 0
            vectors.append(vec_batch[nonzeros, :])
            for i, v in enumerate(index_batch):
                indexes.append(v)
    except:
        traceback.print_exc()

    vectors = np.concatenate(vectors)
    indexes = np.array(indexes, dtype=np.uint64)

    print
    print vectors.shape
    print

    print 'generating ann index'
    index = AnnoyIndex(vectors.shape[1], metric='angular')
    for idx, vector in enumerate(vectors):
	index.add_item(idx, vector)
    index.build(10)

    print 'saving'
    indexes.tofile('%s.indexes' % sys.argv[3])
    index.save(sys.argv[3])
