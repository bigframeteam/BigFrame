#!/usr/bin/env bash

######################### VERTICA RELATED ##########################
# The home directory of Vertica
VERTICA_HOME="/opt/vertica"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.vertica.home=${VERTICA_HOME}"

# The list of hosts in the Vertica cluster
VERTICA_HOSTNAMES=""
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.vertica.hostnames=${VERTICA_HOSTNAMES}"

# The Vertica port
VERTICA_PORT="5433"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.vertica.port=${VERTICA_PORT}"

# The Database used 
VERTICA_DATABASE=""
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.vertica.database=${VERTICA_DATABASE}"

# The user name used
VERTICA_USERNAME=""
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.vertica.username=${VERTICA_USERNAME}"

# The password used
VERTICA_PASSWORD=""
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.vertica.password=${VERTICA_PASSWORD}"

# The JDBC Server
VERTICA_JDBC_SERVER=""
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.vertica.jdbc.server=${VERTICA_JDBC_SERVER}"
