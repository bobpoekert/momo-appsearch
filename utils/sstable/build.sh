#!/bin/sh
gcc -g -O3 -lrt -lpthread -std=c99 shmemq.c -o shmemq.a
gcc -g -O3 -lpthread main.c shmemq.a -o sstable
