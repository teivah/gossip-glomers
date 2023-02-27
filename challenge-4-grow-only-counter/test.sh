#!/bin/bash

cwd=$(pwd)
go build -o bin
cd $MAELSTROM_PATH
./maelstrom test -w g-counter --bin $cwd/bin --node-count 3 --rate 100 --time-limit 20 --nemesis partition
cd $cwd
