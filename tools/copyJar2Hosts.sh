#!/bin/bash

##########################################################
#
# This script can be used to copy jar file to target set
# of hosts
#
# Usage: ./copyJar2Hosts <jar_file> <dest_dir> <hosts_file>
# 
# Author: Ande He
# Date: Feb 13, 2014
#
##########################################################


if [ $# -ne 3 ]
then
	echo "Usage: $0 <jar_file> <dest_dir> <hosts_file>"
	exit -1
fi

JAR_FILE=$1
DEST_DIR=$2
HOSTS_FILE=$3

OIFS=$IFS
IFS=$'\r\n' 
HOSTS=($(cat $HOSTS_FILE))

echo "hosts: ${HOSTS[@]}"

for HOST in ${HOSTS[@]} 
do
	echo $HOST
	scp $JAR_FILE $HOST:$DEST_DIR
done


