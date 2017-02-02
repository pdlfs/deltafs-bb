#!/bin/bash
num_clients=$1
for i in `seq 1 $num_clients`;
do
  ../build/src/bbos_client $i.obj &
done
