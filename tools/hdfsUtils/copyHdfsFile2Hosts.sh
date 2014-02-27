#!/bin/bash

##########################################################
#
# This script can be used to equally distribute hdfs files 
# to a given set of local filesystem
#
# Usage: ./copyHdfsFile2Hosts <hdfs_dir> <local_dir> <hosts_file>
# 
# Author: Ande He
# Date: Feb 13, 2014
#
##########################################################


if [ $# -ne 4 ]
then
	echo "Usage: $0 <hdfs_dir> <intermediate_dir> <local_dir> <hosts_file>"
	exit -1
fi

HDFS_DIR=$1
INTER_DIR=$2
LOCAL_DIR=$3
HOSTS_FILE=$4

OIFS=$IFS
IFS=$'\r\n' 
HOSTS=($(cat $HOSTS_FILE))

echo "hosts: ${HOSTS[@]}"

NUM_HOST=${#HOSTS[@]}

# Get the current, bin, and tools directories
CURRENT_DIR=`pwd`
BIN_DIR=`dirname "$0"`
BIN_DIR=`cd "$BIN_DIR"; pwd`
TOOLS_DIR=`cd "$BIN_DIR/../"; pwd`
cd $CURRENT_DIR;

SPLIT_JAR=$TOOLS_DIR/hdfsUtils/splitHDFSfile.jar

# Re-splits the files into number of hosts

hadoop jar $SPLIT_JAR bigframe.util.SplitHDFSfile $HDFS_DIR $INTER_DIR $NUM_HOST

# Get the set of files in the HDFS directory

FILES=($(hadoop fs -ls $INTER_DIR | grep part-* | awk '{print $NF}'))

if [ ${#FILES[@]} -ne ${#HOSTS[@]} ]; then
	echo ${#FILES[@]} ${#HOSTS[@]} 
	echo "Number of files does not equal to number of hosts."
	echo "Something wrong happen!"
	exit -1
fi

for ((i=0;i<$NUM_HOST;++i))
do
	echo ${HOSTS[i]}
	ssh ${HOSTS[i]} "rm -rf $LOCAL_DIR"
	ssh ${HOSTS[i]} "mkdir -p $LOCAL_DIR" &
	ssh ${HOSTS[i]} "$HADOOP_HOME/bin/hadoop fs -get ${FILES[i]} $LOCAL_DIR" &
done







