#!/bin/bash

gcc-5 -g -c -fopenmp -o quadtree.o quadtree.c
gcc-5 -g -fopenmp -o main quadtree.o main.c
