#!/bin/bash

ITER=1
while true; do
  echo "=== Running TestManyElections3A Iteration $ITER ==="
  VERBOSE=1 go test -run TestManyElections3A -race > raft.log 2>&1

  if grep -q FAIL raft.log; then
    echo "!!! Test failed on iteration $ITER. Dumping logs with ./dslogs -c 7"
    ./dslogs -c 7 < raft.log
    break
  else
    echo "--- Test passed ---"
  fi

  ITER=$((ITER + 1))
done
