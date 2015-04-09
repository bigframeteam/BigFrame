#!/usr/bin/env bash

######################### GIRAPH RELATED ##########################
GIRAPH_NUM_WORKERS=64
BIGFRAME_OPTS="${BIGFRAME_OPTS} -Dbigframe.giraph.num.workers=${GIRAPH_NUM_WORKERS}"
