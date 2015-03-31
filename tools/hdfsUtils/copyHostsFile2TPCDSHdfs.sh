#!/bin/bash

##########################################################
#
# This script can be used to parallel copy tpcds files from 
# a given set of local filesystem to HDFS
#
# Usage: ./copyHostsFiles2TPCDSHdfs.sh <tpcds_local_dir> <tpcds_hdfs_dir> <hosts_file>
# 
# Author: Ande He
# Date: Feb 13, 2014
#
##########################################################

if [ $# -ne 3 ]
then
	echo "Usage: $0 <tpcds_local_dir> <tpcds_hdfs_dir> <hosts_file>"
	exit -1
fi

TPCDS_LOCAL_DIR=$1
TPCDS_HDFS_DIR=$2
HOSTS_FILE=$3

OIFS=$IFS

IFS=$'\r\n'
TABLES=($(ls $TPCDS_LOCAL_DIR))

# Get the current, bin, and tools directories
CURRENT_DIR=`pwd`
BIN_DIR=`dirname "$0"`
BIN_DIR=`cd "$BIN_DIR"; pwd`
TOOLS_DIR=`cd "$BIN_DIR/../"; pwd`
cd $CURRENT_DIR;

COPYHOSTSFILE2HDFS_COMMAND=$TOOLS_DIR/hdfsUtils/copyHostsFiles2HDFS.sh


for TABLE in ${TABLES[@]}
do
	echo "table name: $TABLE"
	$COPYHOSTSFILE2HDFS_COMMAND $TPCDS_LOCAL_DIR/$TABLE $TPCDS_HDFS_DIR/$TABLE $HOSTS_FILE
done

wait
