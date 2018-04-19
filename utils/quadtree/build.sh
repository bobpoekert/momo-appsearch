#!/bin/bash

gcc-5 -c -fopenmp -o quadtree.o quadtree.c
gcc-5 -fopenmp -o main quadtree.o main.c
