#!/bin/bash

./main $1 | vw --lda 20 --lda_D `wc -l $1.names` -k --passes 10 --cache_file /mnt/sata/lda.cache --readable_model $2
