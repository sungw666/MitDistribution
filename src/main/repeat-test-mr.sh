#!/bin/sh

# Usage: sh repeat-test-mr.sh [ITERATIONS]
# Default ITERATIONS=1000

ITER=${1:-1000}
PASS_COUNT=0
FAIL_COUNT=0

TS=`date +%Y%m%d-%H%M%S`
LOG_DIR="repeat-logs-$TS"
mkdir -p "$LOG_DIR"

echo "Will run test-mr.sh $ITER times. Logs in $LOG_DIR" | tee "$LOG_DIR/summary.txt"

i=1
while [ $i -le $ITER ]; do
  echo "=== Run $i / $ITER ===" | tee -a "$LOG_DIR/summary.txt"
  LOG_FILE="$LOG_DIR/run-$i.log"
  # run one iteration, capture all output
  sh test-mr.sh > "$LOG_FILE" 2>&1
  if grep -q "\*\*\* PASSED ALL TESTS" "$LOG_FILE"; then
    PASS_COUNT=$((PASS_COUNT+1))
  else
    FAIL_COUNT=$((FAIL_COUNT+1))
    echo "Run $i FAILED. See $LOG_FILE" | tee -a "$LOG_DIR/summary.txt"
  fi
  i=$((i+1))
done

echo "Total runs: $ITER" | tee -a "$LOG_DIR/summary.txt"
echo "PASSED ALL TESTS: $PASS_COUNT" | tee -a "$LOG_DIR/summary.txt"
echo "FAILED runs: $FAIL_COUNT" | tee -a "$LOG_DIR/summary.txt"

exit 0


