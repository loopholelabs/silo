#!/bin/bash

# Start example_process running without being attached to the current terminal session.

rm test.log

setsid ./example_process < /dev/null &> test.log &

PID=`pgrep example_process`

echo "Running with PID $PID"
