PYINCLUDE := $(shell python -c 'from distutils.sysconfig import get_python_inc; print get_python_inc()')

../programs/pairwise_hashes/pairwise_hashes:
	cd ../programs/pairwise_hashes && make pairwise_hashes

/mnt/zapk/strings.sstable:
		cut -f 3,4 < /mnt/zapk/clean.txt | \
		python ../sstable/build_index.py /mnt/zapk/strings.sstable /mnt/zapk/strings.txt.sstable

pairwise_hashes.bin: ../programs/pairwise_hashes/pairwise_hashes
	../programs/pairwise_hashes/pairwise_hashes pairwise_hashes.bin < /mnt/zapk/clean.txt

variances.so:
	cythonize -i variances.pyx

/mnt/zapk/english_strings.txt: /mnt/zapk/strings.sstable
	python english_strings_text_file.py /mnt/zapk/strings.sstable /mnt/zapk/strings.txt.sstable /mnt/zapk/english_strings.txt

english.sent2vec.bin: /mnt/zapk/english_strings.txt
	../server/sent2vec/fasttext sent2vec -input /mnt/zapk/english_strings.txt -output /mnt/zapk/english.sent2vec \
		 -minCount 8 -dim 600 -epoch 6 -lr 0.2 -wordNgrams 2 -loss ns -neg 10 \
		 -thread 19 -t 0.000005 -dropoutK 4 -minCountLabel 20 -bucket 4000000

ambiguity.bin: pairwise_hashes.bin variances.so
	./ambiguity.py pairwise_hashes.bin translations.bin ambiguity.bin

english.annoy: /mnt/zapk/strings.sstable /mnt/zapk/english.sent2vec.bin
	./word_vectors.py /mnt/zapk/strings.sstable /mnt/zapk/strings.txt.sstable english.annoy /mnt/zapk/english.sent2vec.bin

wiki-news-vec.sstable:
	python ./fasttext_word_vectors.py ./wiki-news-300d-1M-subword.vec ./wiki-news-vec.sstable ./wiki-news-vec.txt.sstable
