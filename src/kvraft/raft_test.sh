#!/bin/bash

testFunc=$1

for num in {1..100}
do
    SERVER_VERBOSE=6 RAFT_VERBOSE=6 go test -run "$1"
done