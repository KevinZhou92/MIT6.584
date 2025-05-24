#!/bin/bash

ITER=1
while true; do
  echo "=== Running TestRPCBytes3B Iteration $ITER ==="
  VERBOSE=1 go test -run TestRPCBytes3B -race > raft.log 2>&1

  if grep -q FAIL raft.log; then
    echo "!!! Test failed on iteration $ITER. Dumping logs with ./dslogs -c 3"
    ./dslogs -c 3 < raft.log
    break
  else
    echo "--- Test passed ---"
  fi

  ITER=$((ITER + 1))
done
