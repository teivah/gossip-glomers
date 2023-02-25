#!/bin/bash

cwd=$(pwd)
rm bin
go build -o bin
cd $MAELSTROM_PATH
./maelstrom test -w broadcast --bin $cwd/bin --node-count 25 --time-limit 20 --rate 100 --latency 100
cd $cwd
