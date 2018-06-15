PYINCLUDE := $(shell python -c 'from distutils.sysconfig import get_python_inc; print get_python_inc()')
PYLIB := $(shell python -c 'import distutils.sysconfig as ss; print ss.get_python_lib()')
NUMPYINCLUDE := $(shell python -c 'import numpy; print numpy.get_include()')


util.a:
	gcc -c -g -fPIC -Ofast -Wall -o util.o util.c
	ar rcs util.a util.o

sstable.so: util.a
	cython -o pysstable.c sstable.pyx
	gcc -g -shared -pthread -L$(PYLIB)/../.. -lpython2.7 -fPIC -fwrapv -Ofast -Wall -fno-strict-aliasing -I$(PYINCLUDE) -I$(NUMPYINCLUDE) -o sstable.so pysstable.c util.a
