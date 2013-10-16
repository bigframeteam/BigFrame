#!/usr/bin/env bash

######################### HADOOP RELATED ##########################
# The HDFS Root Directory to store the generated data
HDFS_ROOT_DIR="/user/andy"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hdfs.root.dir=${HDFS_ROOT_DIR}"

# The Hive HOME Directory
HIVE_HOME=$HIVE_HOME
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hive.home=${HIVE_HOME}"

# The Hive JDBC Server Address
HIVE_JDBC_SERVER="jdbc:hive://localhost:10000/default"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hive.jdbc.server=${HIVE_JDBC_SERVER}"
