#!/usr/bin/env bash

###################################################################
#
# Author: Andy He
# Date:   July 2, 2014
###################################################################

# Get the current, bin, and base directories
CURRENT_DIR=`pwd`
EXP_DIR=`dirname "$0"`
EXP_DIR=`cd "$EXP_DIR"; pwd`
BASE_DIR=`cd "$EXP_DIR/../../"; pwd`
BIN_DIR=$BASE_DIR/bin
cd $CURRENT_DIR;

$BIN_DIR/qgen -mode runqueries -Dmapred.reduce.tasks=64 \
	-Dhive.exec.compress.output=true -Dmapred.output.compression.type=BLOCK \
	-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec




