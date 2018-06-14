#!/bin/bash

gcc -g -ggdb -Ofast -I ../mcdb -o gencdb main.c ../mcdb/libmcdb.a
