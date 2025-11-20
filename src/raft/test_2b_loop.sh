#!/bin/bash

# Counter variables
success=0
fail=0

# Total runs
total=1000

echo "Starting 2B test loop for $total iterations..."

for ((i=1; i<=total; i++))
do
    echo "Running iteration $i/$total..."
    
    # Run the test and capture output
    # Capturing output to a temporary file to check for failure details if needed
    if go test -run 2B > "test_output_${i}.log" 2>&1; then
        ((success++))
        echo "Iteration $i: PASSED"
        rm "test_output_${i}.log" # Remove log if passed
    else
        ((fail++))
        echo "Iteration $i: FAILED"
        echo "Log saved to test_output_${i}.log"
        # Optional: Print the failure output to stdout or just keep the file
    fi
    
    echo "Current Stats - Success: $success, Fail: $fail"
done

echo "Finished $total iterations."
echo "Final Stats - Success: $success, Fail: $fail"


