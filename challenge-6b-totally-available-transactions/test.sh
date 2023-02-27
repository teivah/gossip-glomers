#!/bin/bash

cwd=$(pwd)
go build -o bin
cd $MAELSTROM_PATH
./maelstrom test -w txn-rw-register --bin $cwd/bin --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted
#./maelstrom test -w txn-rw-register --bin $cwd/bin --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
cd $cwd
