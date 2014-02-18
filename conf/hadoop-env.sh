#!/usr/bin/env bash

######################### HADOOP RELATED ##########################
# The HDFS Root Directory to store the generated data
HDFS_ROOT_DIR="hdfs://localhost:9000/bigframe_test"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hdfs.root.dir=${HDFS_ROOT_DIR}"

# The WebHDFS Root Directory to store the generated data
WEBHDFS_ROOT_DIR="http://localhost:50070/webhdfs/v1/user/cszahe/"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.webhdfs.root.dir=${WEBHDFS_ROOT_DIR}"

# The username can access the HDFS_ROOT_DIR
HADOOP_USERNAME="cszahe"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hadoop.username=${HADOOP_USERNAME}"


# The Hive HOME Directory
HIVE_HOME=$HIVE_HOME
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hive.home=${HIVE_HOME}"

# The Hive ORC File Setting
HIVE_ORC="true"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hive.orc=${HIVE_ORC}"

# The Hive HOME Directory
HIVE_WAREHOUSE="hdfs://dbg12:9000/user/hive/warehouse"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hive.warehouse=${HIVE_WAREHOUSE}"

# Snappy Compression
HIVE_SNAPPY="true"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hive.snappy=${HIVE_SNAPPY}"

# Skip Prepare Tables
SKIP_PREPARE_TABLE="false"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.skip.prepare.table=${SKIP_PREPARE_TABLE}"

# The Hive JDBC Server Address
HIVE_JDBC_SERVER="jdbc:hive://localhost:10000/default"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hive.jdbc.server=${HIVE_JDBC_SERVER}"
