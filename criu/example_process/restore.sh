#!/bin/bash

CRIU_BINARY=/home/jimmy/criu/usr/local/sbin/criu

rm restore/*.img restore/stats*
cp images/last/* restore/
cp ../../outputs/* restore/

$CRIU_BINARY restore --images-dir restore --lazy-pages
