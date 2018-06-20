PYINCLUDE := $(shell python -c 'from distutils.sysconfig import get_python_inc; print get_python_inc()')

pymcdb.a:
	gcc -c -fPIC -I ../utils/mcdb -o pymcdb.o pymcdb.c
	ar rcs pymcdb.a pymcdb.o

pybuild: pymcdb.a
	cython -o mcdb.c mcdb.pyx
	gcc -shared -pthread -fPIC -fwrapv -Ofast -Wall -I$(PYINCLUDE) -o mcdb.so mcdb.c pymcdb.a ../utils/mcdb/libmcdb.a

english.mcdb:
	gunzip -c strings.gz | \
		cut -f 3,4 |\
		grep -E $$'^(\t)|^(\s*\-en)' |\
		../utils/gencdb/gencdb english.mcdb
	mcdbctl uniq english.mcdb

all_strings.mcdb:
	gunzip -c strings.gz | \
		cut -f 3,4 | \
		../utils/gencdb/gencdb all_strings.mcdb
	mcdbctl uniq all_strings.mcdb

pairwise_hashes.bin:
	lein with-profile pairwise-text-hashes run `pwd`/strings.gz `pwd`/pairwise_hashes.bin

variances.so:
	cythonize -i variances.pyx

ambiguity: pairwise_hashes.bin variances.so
	./ambiguity.py pairwise_hashes.bin translations.bin ambiguity.bin

word_vectors: english.mcdb mcdb.so
	./word_vectors.py english.mcdb english.annoy english_annoy_hashes.bin
