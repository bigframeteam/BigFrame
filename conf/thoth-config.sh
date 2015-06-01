#!/usr/bin/env bash

ADD_LISTENER="true"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.thoth.addlistener=${ADD_LISTENER}"

METADATA_DB_NAME="thoth"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.thoth.db.name=${METADATA_DB_NAME}"

METADATA_DB_CONNECTION="jdbc:mysql://localhost:3306/"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.thoth.db.connection=${METADATA_DB_CONNECTION}"

METADATA_DB_USERNAME="thoth"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.thoth.db.username=${METADATA_DB_USERNAME}"

METADATA_DB_PASSWORD="thoth"
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.thoth.db.password=${METADATA_DB_PASSWORD}"
