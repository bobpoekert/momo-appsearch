PYINCLUDE := $(shell python -c 'from distutils.sysconfig import get_python_inc; print get_python_inc()')

pymcdb.a:
	gcc -c -fPIC -I ../utils/mcdb -o pymcdb.o pymcdb.c
	ar rcs pymcdb.a pymcdb.o

pybuild: pymcdb.a
	cython -o mcdb.c mcdb.pyx
	gcc -shared -pthread -fPIC -fwrapv -Ofast -Wall -I$(PYINCLUDE) -o mcdb.so mcdb.c pymcdb.a ../utils/mcdb/libmcdb.a

all_strings.sstable:
	gunzip -c strings.gz | \
		cut -f 3,4 | \
		python ../sstable/build_index.py strings.sstable strings.txt.sstable

pairwise_hashes.bin:
	lein with-profile pairwise-text-hashes run `pwd`/strings.gz `pwd`/pairwise_hashes.bin

variances.so:
	cythonize -i variances.pyx

ambiguity: pairwise_hashes.bin variances.so
	./ambiguity.py pairwise_hashes.bin translations.bin ambiguity.bin

word_vectors: all_strings.sstable
	./word_vectors.py all_strings.sstable english.annoy english_annoy_hashes.bin
