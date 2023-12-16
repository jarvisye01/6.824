#!/bin/bash

testFunc=$1

for num in {1..100}
do
    go test -run "$1"
done
