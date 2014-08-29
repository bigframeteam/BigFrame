#!/usr/bin/env bash

###################################################################
#
# Author: Andy He
# Date:   July 2, 2014
###################################################################

# Get the current, bin, and base directories
CURRENT_DIR=`pwd`
EXP_DIR=`dirname "$0"`
EXP_DIR=`cd "$EXP_DIR"; pwd`
BASE_DIR=`cd "$EXP_DIR/../../"; pwd`
BIN_DIR=$BASE_DIR/bin
LOG_DIR=$BASE_DIR/logs
CONF_DIR=$BASE_DIR/conf
cd $CURRENT_DIR;


sed -i 's/SKIP_PREPARE_TABLE="true"/SKIP_PREPARE_TABLE="false"/g' $CONF_DIR/hadoop-env.sh

sed -i 's/SHARK_ENABLE_SNAPPY="false"/SHARK_ENABLE_SNAPPY="true"/g' $CONF_DIR/spark-env.sh

sed -i 's/IMPALA_HIVE_FILEFORMAT="text"/IMPALA_HIVE_FILEFORMAT="mixed"/g' $CONF_DIR/impala-env.sh

$BIN_DIR/qgen -mode runqueries -Dhive.auto.convert.join=false -Dhive.exec.parallel=true \
	    -Dmapred.job.reuse.jvm.num.tasks=-1 -Dio.sort.mb=500 \
		    -Dmapred.child.java.opts=-Xmx4G \
			    -Dmapred.compress.map.output=true -Dmapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec > $LOG_DIR/impalahive-16-2T.log 2>&1


sed -i 's/SKIP_PREPARE_TABLE="true"/SKIP_PREPARE_TABLE="false"/g' $CONF_DIR/hadoop-env.sh

sed -i 's/SHARK_ENABLE_SNAPPY="true"/SHARK_ENABLE_SNAPPY="false"/g' $CONF_DIR/spark-env.sh
#
#$BIN_DIR/qgen -mode runqueries -Dhive.auto.convert.join=false -Dhive.exec.parallel=true \
	#   -Dmapred.job.reuse.jvm.num.tasks=-1 -Dio.sort.mb=500 \
	#    -Dmapred.child.java.opts=-Xmx4G \
	#   -Dmapred.compress.map.output=true -Dmapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec > $LOG_DIR/impalahive-16-2T-withoutSnappy.log 2>&1
#

sed -i 's/SKIP_PREPARE_TABLE="true"/SKIP_PREPARE_TABLE="false"/g' $CONF_DIR/hadoop-env.sh

sed -i 's/SHARK_ENABLE_SNAPPY="true"/SHARK_ENABLE_SNAPPY="false"/g' $CONF_DIR/spark-env.sh

sed -i 's/IMPALA_HIVE_FILEFORMAT="mixed"/IMPALA_HIVE_FILEFORMAT="text"/g' $CONF_DIR/impala-env.sh

#$BIN_DIR/qgen -mode runqueries -Dhive.auto.convert.join=false -Dhive.exec.parallel=true \
	#   -Dmapred.job.reuse.jvm.num.tasks=-1 -Dio.sort.mb=500 \
	#    -Dmapred.child.java.opts=-Xmx4G \
	#   -Dmapred.compress.map.output=true -Dmapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec > $LOG_DIR/impalahive-16-2T-withoutORCParquet.log 2>&1


