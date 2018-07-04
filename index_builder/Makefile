PYINCLUDE := $(shell python -c 'from distutils.sysconfig import get_python_inc; print get_python_inc()')
PYLIB := $(shell python -c 'import distutils.sysconfig as ss; print ss.get_python_lib()')
NUMPYINCLUDE := $(shell python -c 'import numpy; print numpy.get_include()')

../programs/pairwise_hashes/pairwise_hashes:
	cd ../programs/pairwise_hashes && make pairwise_hashes

../sstable/sstable.so:
	cd ../sstable && make sstable.so

/mnt/zapk/raw_strings.sstable:
	cut -f 3,4 < /mnt/wd/strings_uniq.txt |\
		python ../sstable/build_index.py /mnt/zapk/raw_strings.sstable /mnt/zapk/raw_strings.txt.sstable

clean_text.so:
	cythonize -i clean_text.pyx

/mnt/zapk/clean.txt: ../sstable/sstable.so clean_text.so
	python _clean_text.py words.sstable words.txt.sstable /mnt/zapk/clean_hashes.bin.tmp /mnt/wd/strings_uniq.txt /mnt/zapk/clean.txt /mnt/zapk/english_hashes.bin.tmp
	python clean_hashes_index.py /mnt/zapk/clean_hashes.bin.tmp clean_hashes.bin /mnt/zapk/english_hashes.bin.tmp english_hashes.bin
	#rm /mnt/zapk/clean_hashes.bin.tmp
	#rm /mnt/zapk/english_hashes.bin.tmp

/mnt/zapk/strings.sstable: /mnt/zapk/clean.txt ../sstable/sstable.so
	cut -f 3 < /mnt/zapk/clean.txt | \
		python ../sstable/build_index.py /mnt/zapk/strings.sstable /mnt/zapk/strings.txt.sstable

pairwise_hashes.bin: ../programs/pairwise_hashes/pairwise_hashes ../sstable/sstable.so /mnt/zapk/clean.txt
	../programs/pairwise_hashes/pairwise_hashes pairwise_hashes.bin < /mnt/zapk/clean.txt

variances.so:
	cython -o variances.c variances.pyx
	gcc -g -shared -pthread -L$(PYLIB)/../.. -lpython2.7 -fPIC -fwrapv -Ofast -Wall -fno-strict-aliasing -I$(PYINCLUDE) -I$(NUMPYINCLUDE) -o variances.so variances.c

/mnt/zapk/english_strings.txt: /mnt/zapk/strings.sstable
	python english_strings_text_file.py /mnt/zapk/strings.sstable /mnt/zapk/strings.txt.sstable /mnt/zapk/english_strings.txt english_hashes.bin

/mnt/zapk/english.sent2vec.bin: /mnt/zapk/english_strings.txt
	#../server/sent2vec/fasttext sent2vec -input /mnt/zapk/english_strings.txt -output /mnt/zapk/english.sent2vec \
	#	 -minCount 8 -dim 600 -epoch 6 -lr 0.2 -wordNgrams 2 -loss ns -neg 10 \
	#	 -thread 19 -t 0.000005 -dropoutK 4 -minCountLabel 20 -bucket 4000000

ambiguity.bin: pairwise_hashes.bin variances.so
	./ambiguity.py pairwise_hashes.bin ambiguity.bin translations.sstable translations.txt.sstable

english.annoy: /mnt/zapk/strings.sstable /mnt/zapk/english.sent2vec.bin
	./word_vectors.py /mnt/zapk/strings.sstable /mnt/zapk/strings.txt.sstable english.annoy /mnt/zapk/english.sent2vec.bin english_hashes.bin

wiki-news-vec.sstable:
	python ./fasttext_word_vectors.py ./wiki-news-300d-1M-subword.vec ./wiki-news-vec.sstable ./wiki-news-vec.txt.sstable

.PHONY: demo_deps
demo_deps: english.annoy ambiguity.bin /mnt/zapk/english.sent2vec.bin /mnt/zapk/strings.sstable /mnt/zapk/raw_strings.sstable ../sstable/sstable.so

demo: demo_deps
	python demo.py

test: demo_deps
	python test.py
