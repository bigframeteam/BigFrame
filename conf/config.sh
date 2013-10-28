#!/usr/bin/env bash

###################################################################
# The BigFrame configuration parameters
#
# Used to set the user-defined parameters in BigFrame.
#
# Author: Andy He
# Date:   June 16, 2013
###################################################################

###################################################################
# GLOBAL PARAMETERS USED BY DATA GENERATOR (REQUIRED)
###################################################################

# The Hadoop Home Directory
HADOOP_HOME=$HADOOP_HOME
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hadoop.home=${HADOOP_HOME}"

# The Hadoop slave file
HADOOP_SLAVES=$HADOOP_HOME/conf/slaves
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hadoop.slaves=${HADOOP_SLAVES}"

# Local Directory to store the temporary TPCDS generated files
TPCDS_LOCAL=~/tpcds_tmp
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.tpcds.local=${TPCDS_LOCAL}"

# Local Directory to store the itermediate data used for data refershing
REFRESH_LOCAL=~/bigframe_refresh_data
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.refresh.local=${REFRESH_LOCAL}"

# Global Output Path
export OUTPUT_PATH=""



