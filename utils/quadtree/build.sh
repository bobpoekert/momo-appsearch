#!/bin/bash

gcc-5 -g -O3 -c -fopenmp -o quadtree.o quadtree.c
gcc-5 -g -O3 -fopenmp -o main quadtree.o main.c
