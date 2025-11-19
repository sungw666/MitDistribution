#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_FILE="$SCRIPT_DIR/test_run.log"

: > "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

success=0
fail=0

for i in {1..5000}
do
   echo "Running test iteration $i"
   if go test -run 2A; then
       success=$((success + 1))
   else
       echo "Test failed on iteration $i"
       fail=$((fail + 1))
   fi
done

echo "Test runs completed. Successes: $success, Failures: $fail"
