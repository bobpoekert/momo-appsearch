PYINCLUDE := $(shell python -c 'from distutils.sysconfig import get_python_inc; print get_python_inc()')
PYLIB := $(shell python -c 'import distutils.sysconfig as ss; print ss.get_python_lib()')
NUMPYINCLUDE := $(shell python -c 'import numpy; print numpy.get_include()')

clean:
	rm clean_tokens.a
	rm clean_tokens.o
	rm text_utils.so

clean_tokens.a:
	gcc -c -g -fPIC -Ofast -Wall -o clean_tokens.o clean_tokens.c
	ar rcs clean_tokens.a clean_tokens.o

text_utils.so: clean_tokens.a
	cython -o text_utils.c text_utils.pyx
	gcc -g -shared -pthread -L$(PYLIB)/../.. -lpython2.7 -fPIC -fwrapv -Ofast -Wall -fno-strict-aliasing -I$(PYINCLUDE) -I$(NUMPYINCLUDE) -o text_utils.so text_utils.c clean_tokens.a
