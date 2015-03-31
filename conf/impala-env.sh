#!/usr/bin/env bash

######################### IMPALA RELATED ##########################
IMPALA_HOME="set"

IMPALA_JDBC_SERVER="jdbc:hive2://dbg03:21050/;auth=noSasl"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.impala.jdbc.server=${IMPALA_JDBC_SERVER}"
