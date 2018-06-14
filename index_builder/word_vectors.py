#!/usr/bin/env python2.7

import sent2vec
from annoy import AnnoyIndex
import mcdb
import numpy as np
from itertools import izip

if __name__ == '__main__':
    import sys

    print 'loading strings'
    with open(sys.argv[1], 'r') as inf:
        stringlist = list(mcdb.db_data(inf))

    print 'loading vec model'
    vec_model = sent2vec.Sent2vecModel()
    vec_model.load_model('./vec_model.bin')

    print 'embedding strings'
    vectors = vec_model.embed_sentences([v[1] for v in stringlist])

    print 'generating ann index'
    index = AnnoyIndex(vectors.shape[1], metric='angular')
    id_keys = []
    for (k, string), vector in izip(stringlist, vectors):
	idx = len(id_keys)
	id_keys.append(k)
	index.add_item(idx, vector)
    index.build(10)

    print 'saving'
    index.save(sys.argv[2])
    np.array(id_keys, dtype=np.uint32).tofile(sys.argv[3])
