#!/bin/bash

cwd=$(pwd)
go build -o bin
cd $MAELSTROM_PATH
./maelstrom test -w echo --bin $cwd/bin --node-count 1 --time-limit 10
cd $cwd
