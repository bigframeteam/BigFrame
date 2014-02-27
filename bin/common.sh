#!/usr/bin/env bash

###################################################################
# A script that loads the configuration settings from conf/config.sh
# and performs various initializations and error checks.
#
# The following parameters are created:
#   CURRENT_DIR
#   BIN_DIR
#   BASE_DIR
#   JARS_DIR
#   CONF_DIR
#   BIGFRAME_OPTS
#
# The user should not modify this script. All user-defined
# parameters can be set in 'conf/config.sh'.
#
# Author: Andy He
# Date:   Jun 17, 2013
###################################################################

# Get the current, bin, and base directories
CURRENT_DIR=`pwd`
BIN_DIR=`dirname "$0"`
BIN_DIR=`cd "$BIN_DIR"; pwd`
BASE_DIR=`cd "$BIN_DIR/../"; pwd`
cd $CURRENT_DIR;

TPCDS_SCRIPT=$BASE_DIR/tools/tpcds/gen_data.pl
TPCDS_UPDATE_SCRIPT=$BASE_DIR/tools/tpcds/gen_update_data.pl
GEN_PROMTTBL_SCRIPT=$BASE_DIR/tools/tpcds/gen_single_tbl.pl

CONF_DIR=$BASE_DIR/conf

# Get the user-defined parameters
. "$BASE_DIR"/conf/config.sh

# Get the user-defined hadoop parameters
. "$BASE_DIR"/conf/hadoop-env.sh

# Get the user-defined spark parameters
. "$BASE_DIR"/conf/spark-env.sh

# Get the user-defined vertica parameters
. "$BASE_DIR"/conf/vertica-env.sh

# Get the user-defined vertica parameters
. "$BASE_DIR"/conf/impala-env.sh


BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.tpcds.script=${TPCDS_SCRIPT}"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.tpcds.updatescript=${TPCDS_UPDATE_SCRIPT}"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.singletblgen.script=${GEN_PROMTTBL_SCRIPT}"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.conf.dir=${CONF_DIR}"


COMMON_JAR=`ls $BASE_DIR/common/target/scala-2.9.3/bigframe-common-assembly*.jar`

DATAGEN_JAR=`ls $BASE_DIR/datagen/target/scala-2.9.3/bigframe-datagen-assembly*.jar`

QGEN_JAR=`ls $BASE_DIR/qgen/target/scala-2.9.3/bigframe-qgen-assembly*.jar`

WORKFLOWS_JAR=`ls $BASE_DIR/workflows/target/scala-2.9.3/bigframe-workflows-assembly*.jar`

# Please do not remove the following, Spark's engine driver requires this jar while launching a standalone application
export WORKFLOWS_JAR

# UDF used by Hive and Shark
UDF_JAR=$WORKFLOWS_JAR

BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.udf.jar=${UDF_JAR}"

#SENTIMENT_JAR= `ls $BASE_DIR/sentiment/target/scala-2.9.3/bigframe-workflows-assembly*.jar`
