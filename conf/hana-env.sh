#!/usr/bin/env bash

######################### HANA RELATED ##########################
# The home directory of Hana
HANA_HOME="/hana"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hana.home=${HANA_HOME}"

# The list of hosts in the HANA cluster
HANA_HOSTNAMES="dbg12"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hana.hostnames=${HANA_HOSTNAMES}"

# The HANA port
HANA_PORT="30015"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hana.port=${HANA_PORT}"

# The Database used 
HANA_DATABASE="bigframe"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hana.database=${HANA_DATABASE}"

# The user name used
HANA_USERNAME="dbadmin"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hana.username=${HANA_USERNAME}"

# The password used
HANA_PASSWORD="bigfrmae"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hana.password=${HANA_PASSWORD}"

# The JDBC Server
HANA_JDBC_SERVER="jdbc:sap://localhost:30015/bigframe"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.hana.jdbc.server=${HANA_JDBC_SERVER}"
