PYINCLUDE := $(shell python -c 'from distutils.sysconfig import get_python_inc; print get_python_inc()')
PYLIB := $(shell python -c 'import distutils.sysconfig as ss; print ss.get_python_lib()')
NUMPYINCLUDE := $(shell python -c 'import numpy; print numpy.get_include()')

clean:
	rm text_utils.so

../util/util.o:
	cd ../util && make util.o

text_utils.so: ../util/util.o
	cython -o text_utils.c text_utils.pyx
	gcc -g -shared -pthread -L$(PYLIB)/../.. -lpython2.7 -fPIC -fwrapv -Ofast -Wall -fno-strict-aliasing -I$(PYINCLUDE) -I$(NUMPYINCLUDE) -I ../util -o text_utils.so text_utils.c ../util/util.o

test: text_utils.so
	python test.py
