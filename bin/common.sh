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
TWEET_SAMPLE_DATA=$BASE_DIR/contrib/datagen/sample_tweet.json
SCALA_VERSION="2.10"
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

# Get the user-defined vertica parameters
. "$BASE_DIR"/conf/giraph-env.sh


BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.tpcds.script=${TPCDS_SCRIPT}"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.tpcds.updatescript=${TPCDS_UPDATE_SCRIPT}"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.singletblgen.script=${GEN_PROMTTBL_SCRIPT}"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.conf.dir=${CONF_DIR}"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.sample.tweet.path=${TWEET_SAMPLE_DATA}"

COMMON_JAR=`ls $BASE_DIR/common/target/scala-${SCALA_VERSION}/bigframe-common-assembly*.jar`

DATAGEN_JAR=`ls $BASE_DIR/datagen/target/scala-${SCALA_VERSION}/bigframe-datagen-assembly*.jar`

QGEN_JAR=`ls $BASE_DIR/qgen/target/scala-${SCALA_VERSION}/bigframe-qgen-assembly*.jar`

WORKFLOWS_JAR=`ls $BASE_DIR/workflows/target/scala-${SCALA_VERSION}/bigframe-workflows-assembly*.jar`

# Please do not remove the following, Spark's engine driver requires this jar while launching a standalone application
export WORKFLOWS_JAR

# UDF used by Hive and Shark
UDF_JAR=$WORKFLOWS_JAR
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.udf.jar=${UDF_JAR}"

# Check which system is used in the workflow
HADOOP_IN_USE=false
SPARK_IN_USE=false
VERTICA_IN_USE=false
IMPALA_IN_USE=false
SHARK_IN_USE=false
HIVE_IN_USE=false
GIRAPH_IN_USE=false

BIGFRAME_CORE_XML=$BASE_DIR/conf/bigframe-core.xml
VALUES=$(xmllint --shell $BIGFRAME_CORE_XML <<<"cat /configuration/property/value/text()" | grep -v "^/ >") 

if [[ $VALUES == *hadoop* ]]
then
	echo "Hadoop is used";
	HADOOP_IN_USE=true
fi

if [[ $VALUES == *spark* ]]
then
	echo "Spark is used";
	SPARK_IN_USE=true
fi

if [[ $VALUES == *vertica* ]]
then
	echo "Vertica is used";
	VERTICA_IN_USE=true
fi

if [[ $VALUES == *impala* ]]
then
	echo "Impala is used";
	IMPALA_IN_USE=true
fi

if [[ $VALUES == *shark* ]]
then
	echo "Shark is used";
	SHARK_IN_USE=true
fi

if [[ $VALUES == *hive* ]]
then
	echo "Hive is used";
	HIVE_IN_USE=true
fi

if [[ $VALUES == *giraph* ]]
then
	echo "Giraph is used";
	GIRAPH_IN_USE=true
fi


