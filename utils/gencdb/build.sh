#!/bin/bash

gcc -g -ggdb -O3 -I ../mcdb -o gencdb main.c ../mcdb/libmcdb.a
