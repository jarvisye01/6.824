#!/bin/bash

testFunc=$1

for num in {1..100}
do
    # VERBOSE=1 go test -run TestReElection2A
    VERBOSE=6 go test -run "$1"
done
