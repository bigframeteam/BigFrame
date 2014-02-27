#!/bin/bash

##########################################################
#
# This script can be used to equally distribute all the TPCDS
# tables in HDFS to a given set of local filesystem
#
# Usage: ./copyTPCDSHdfsFile2Hosts <tpcds_hdfs_dir> <tpcds_local_dir> <hosts_file>
# 
# Author: Ande He
# Date: Feb 13, 2014
#
##########################################################

if [ $# -ne 3 ]
then
	echo "Usage: $0 <tpcds_hdfs_dir> <tpcds_local_dir> <hosts_file>"
	exit -1
fi

TPCDS_HDFS_DIR=$1
TPCDS_LOCAL_DIR=$2
HOSTS_FILE=$3

OIFS=$IFS

IFS=$'\r\n'
TABLES=($(hadoop fs -ls $TPCDS_HDFS_DIR | awk '{if(NF > 3) print $NF}'))


# Get the current, bin, and tools directories
CURRENT_DIR=`pwd`
BIN_DIR=`dirname "$0"`
BIN_DIR=`cd "$BIN_DIR"; pwd`
TOOLS_DIR=`cd "$BIN_DIR/../"; pwd`
cd $CURRENT_DIR;

COPYHDFSFILE2HOST_COMMAND=$TOOLS_DIR/hdfsUtils/copyHdfsFile2Hosts.sh

PREFIX="intermediate_split_dir"

for TABLE in ${TABLES[@]}
do
	TABLE_NAME=($(basename $TABLE))
	echo "Table name: $TABLE_NAME"
	INTER_DIR=$PREFIX/$TABLE_NAME
	$COPYHDFSFILE2HOST_COMMAND $TABLE $INTER_DIR $TPCDS_LOCAL_DIR/$TABLE_NAME $HOSTS_FILE
done


