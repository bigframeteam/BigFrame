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

$BIN_DIR/dataload -from hadoop -to vertica -src relational_data2048 \
	-appdomain BI -datatype relational
$BIN_DIR/dataload -from hadoop -to vertica -src graph_data2048 \
	-appdomain BI -datatype graph
$BIN_DIR/dataload -from hadoop -to vertica -src nested_data2048 \
	-appdomain BI -datatype nested -Dtweet.store.format=normalized



