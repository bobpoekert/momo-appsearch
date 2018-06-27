PYINCLUDE := $(shell python -c 'from distutils.sysconfig import get_python_inc; print get_python_inc()')
PYLIB := $(shell python -c 'import distutils.sysconfig as ss; print ss.get_python_lib()')
NUMPYINCLUDE := $(shell python -c 'import numpy; print numpy.get_include()')

clean:
	rm sstable.so

../util/util.o:
	cd ../util && make util.o

sstable.so: ../util/util.o
	cython -o pysstable.c sstable.pyx
	gcc -g -shared -pthread -L$(PYLIB)/../.. -lpython2.7 -fPIC -fwrapv -Ofast -Wall -fno-strict-aliasing -I$(PYINCLUDE) -I$(NUMPYINCLUDE) -I../util -o sstable.so pysstable.c ../util/util.o

test: sstable.so
	python test.py
