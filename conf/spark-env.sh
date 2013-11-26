#!/usr/bin/env bash

######################### SPARK RELATED ##########################

# Path of Spark installation home. 
export SPARK_HOME="/usr/spark-0.8.0"

# Path of Scala installation home.
export SCALA_HOME="/usr/scala-2.9.3"

# Spark memory parameters, defaults will be used if unspecified
# Make sure that SPARK_WORKER_MEM and SPARK_WORKER_CORES are set 
# to their optimal values in the spark_env.sh within Spark installation
# configuration directory. Only application specific parameters like 
# SPARK_MEM will be set here.
export SPARK_MEM=4g

# Spark connection string, available in Spark master's webUI
export SPARK_CONNECTION_STRING="spark://ubuntu:7077"

# The Spark Home Directory
SPARK_HOME=$SPARK_HOME
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.spark.home=${SPARK_HOME}"

# The Shark Home
SHARK_HOME=$SHARK_HOME
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.shark.home=${SHARK_HOME}"

# Local directory for Spark scratch space
SPARK_LOCAL_DIR="/tmp/spark_local"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.spark.local.dir=${SPARK_LOCAL_DIR}"

# Use bagel for Spark
SPARK_USE_BAGEL=true
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.spark.usebagel=${SPARK_USE_BAGEL}"

# Spark degree of parallelism
SPARK_DOP=8
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.spark.dop=${SPARK_DOP}"

# The Spark Master Address
SPARK_MASTER=$SPARK_CONNECTION_STRING
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.spark.master=${SPARK_MASTER}"

# Global Output Path
export OUTPUT_PATH="hdfs://localhost:9000/test_output"

