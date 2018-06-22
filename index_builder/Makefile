PYINCLUDE := $(shell python -c 'from distutils.sysconfig import get_python_inc; print get_python_inc()')

pymcdb.a:
	gcc -c -fPIC -I ../utils/mcdb -o pymcdb.o pymcdb.c
	ar rcs pymcdb.a pymcdb.o

pybuild: pymcdb.a
	cython -o mcdb.c mcdb.pyx
	gcc -shared -pthread -fPIC -fwrapv -Ofast -Wall -I$(PYINCLUDE) -o mcdb.so mcdb.c pymcdb.a ../utils/mcdb/libmcdb.a

strings.sstable:
	gunzip -c strings.gz | \
		cut -f 3,4 | \
		python ../sstable/build_index.py strings.sstable strings.txt.sstable

pairwise_hashes.bin:
	lein with-profile pairwise-text-hashes run `pwd`/strings.gz `pwd`/pairwise_hashes.bin

variances.so:
	cythonize -i variances.pyx

english_strings.txt: strings.sstable
	python ./english_strings_text_file.py strings.sstable strings.txt.sstable english_strings.txt

english.sent2vec.bin: english_strings.txt
	../server/sent2vec/fasttext sent2vec -input english_strings.txt -output english.sent2vec \
		 -minCount 8 -dim 600 -epoch 6 -lr 0.2 -wordNgrams 2 -loss ns -neg 10 \
		 -thread 19 -t 0.000005 -dropoutK 4 -minCountLabel 20 -bucket 4000000

ambiguity.bin: pairwise_hashes.bin variances.so
	./ambiguity.py pairwise_hashes.bin translations.bin ambiguity.bin

english.annoy: strings.sstable english.sent2vec.bin
	./word_vectors.py strings.sstable strings.txt.sstable english.annoy english.sent2vec.bin
