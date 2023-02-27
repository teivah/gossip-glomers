#!/bin/bash

cwd=$(pwd)
go build -o bin
cd $MAELSTROM_PATH
./maelstrom test -w txn-rw-register --bin $cwd/bin --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
cd $cwd
