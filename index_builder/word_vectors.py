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

if __name__ == '__main__':
    import sys

    print 'loading vec model'
    vec_model = sent2vec.Sent2vecModel()
    vec_model.load_model('./vec_model.bin')

    print 'embedding strings'
    vectors = []
    indexes = []
    index_batch = []
    string_batch = []
    for idx, v in enumerate(sstable.SSTable(sys.argv[1], sys.argv[2]).raw_itervalues()):
        try:
            if v[0] == '-' and not v.startswith('-en'):
                continue
            v = v.split('\t')[1]
            try:
                string_batch.append(tt.clean_tokens(v))
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
                        indexes.append(i)
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
                indexes.append(i)
    except:
        traceback.print_exc()

    vectors = np.concatenate(vectors)

    print 'generating ann index'
    index = AnnoyIndex(vectors.shape[1], metric='angular')
    for idx, vector in izip(indexes, vectors):
	index.add_item(idx, vector)
    index.build(10)

    print 'saving'
    index.save(sys.argv[3])
