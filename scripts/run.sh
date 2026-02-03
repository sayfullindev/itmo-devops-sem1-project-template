#!/bin/bash


# step 1: compilate go
if ! go build -o server main.go; then
    echo "error while compiling go"
    exit 1
fi


# step 2: starting server
echo "starting server on port :8080"
./server &