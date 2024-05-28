#!/bin/bash

CRIU_BINARY=/home/jimmy/criu/usr/local/sbin/criu
PS_ADDR="127.0.0.1"
PS_PORT="5171"

TARGET_PID=`pgrep example_process`

NOW=`date +"%s"`

if [ -d "images" ]; then
  echo "Iterative pre-dump [$NOW]..."
  mkdir images/images_$NOW
  mv images/last/* images/images_$NOW

  $CRIU_BINARY pre-dump --tree $TARGET_PID --images-dir images/last --prev-images-dir ../images_$NOW --track-mem \
  --page-server --address $PS_ADDR --port $PS_PORT
else
  echo "Creating first image [$NOW]..."
  mkdir images
  mkdir images/last
  $CRIU_BINARY pre-dump --tree $TARGET_PID --images-dir images/last --track-mem \
  --page-server --address $PS_ADDR --port $PS_PORT
fi
