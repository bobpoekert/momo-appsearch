
../../util/util.o:
	cd ../../util; make util.o

pairwise_hashes: ../../util/util.o
	gcc -g -Ofast -Wall -I../../util -o pairwise_hashes main.c ../../util/util.o

test: pairwise_hashes
	python test.py
