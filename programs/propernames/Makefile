
../sstable/libsstable.a:
	cd ../sstable && make libsstable.a

propernames: ../sstable/libsstable.a
	gcc -g -Ofast -Wall -I../util -I../sstable -o propernames propernames.c ../sstable/libsstable.a
