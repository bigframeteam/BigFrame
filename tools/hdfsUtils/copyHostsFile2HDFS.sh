#!/bin/bash

##########################################################
#
# This script can be used to parallel copy files from a given set of
# local filesystem to HDFS
#
# Usage: ./copyHostsFiles2HDFS <local_dir> <hdfs_dir> <hosts_file>
# 
# Author: Ande He
# Date: Feb 13, 2014
#
##########################################################


if [ $# -ne 3 ]
then
	echo "Usage: $0 <local_dir> <hdfs_dir> <hosts_file>"
	exit -1
fi

LOCAL_DIR=$1
HDFS_DIR=$2
HOSTS_FILE=$3

OIFS=$IFS
IFS=$'\r\n' 
HOSTS=($(cat $HOSTS_FILE))

echo "hosts: ${HOSTS[@]}"

hadoop fs -rmr $HDFS_DIR
hadoop fs -mkdir $HDFS_DIR

for host in ${HOSTS[@]}
do
	ssh $host "$HADOOP_HOME/bin/hadoop fs -put $LOCAL_DIR/* $HDFS_DIR" &
done

wait


