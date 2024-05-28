#!/bin/bash

# Start example_process running without being attached to the current terminal session.

setsid ./example_process < /dev/null &> test.log &
