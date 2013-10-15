#!/usr/bin/env bash

######################### SPARK RELATED ##########################

# Path of Spark installation home. 
export SPARK_HOME=""

# Path of Scala installation home.
export SCALA_HOME=""

# Spark memory parameters, defaults will be used if unspecified
# export SPARK_MEM=6g
# export SPARK_WORKER_MEMORY=6g

# Spark connection string, available in Spark master's webUI
export SPARK_CONNECTION_STRING="spark://localhost:7077"

# The Spark Home Directory
SPARK_HOME=$SPARK_HOME

# The Shark Home
SHARK_HOME=$SHARK_HOME
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.shark.home=${SHARK_HOME}"

# The Spark Master Address
SPARK_MASTER=$SPARK_CONNECTION_STRING
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.spark.master=${SPARK_MASTER}"


