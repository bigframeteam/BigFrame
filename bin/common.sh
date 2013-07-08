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
# The user should not need to modify this script. All user-defined
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

JARS_DIR=$BASE_DIR/jars

TPCDS_SCRIPT=$BASE_DIR/tools/tpcds/gen_data.pl
GEN_PROMTTBL_SCRIPT=$BASE_DIR/tools/tpcds/gen_promt_tbl.pl

CONF_DIR=$BASE_DIR/conf

# Get the user-defined parameters
. "$BASE_DIR"/conf/config.sh

BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.tpcds.script=${TPCDS_SCRIPT}"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.promttblgen.script=${GEN_PROMTTBL_SCRIPT}"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.conf.dir=${CONF_DIR}"



