#!/usr/bin/env bash

set -e -o

# This is required for using dx commands
DX_ENV_FILE=/etc/profile.d/dnanexus.environment.sh
if [[ -f "$DX_ENV_FILE" ]]; then
    # This file need not be sourced in python3 env
    source $DX_ENV_FILE
fi
source /cluster/dx-cluster.environment
source /home/dnanexus/environment

ERROR_FILE=/home/dnanexus/dx_stderr
OUT_FILE=/home/dnanexus/dx_stdout

if [ -z "$1" ]; then
  ITER=0
else
  ITER=$1
fi

if [ -z "$2" ]; then
  COMPRESS=0
else
  COMPRESS=$2
fi

if [ -z "$3" ]; then
  FOLDER=''
else
  FOLDER=$3
fi


if [ "$DX_CLUSTER_NODE_ID" -eq 0 ]; then
  NODE_NAME="driver"
else
  NODE_NAME="worker_$DX_CLUSTER_NODE_ID"
fi

DEST="/$FOLDER/$DX_JOB_ID/$NODE_NAME"
dx mkdir -p "$DX_PROJECT_CONTEXT_ID:/$DEST"
mkdir -p /cluster/logger/eventlogs

if [ "$DX_CLUSTER_NODE_ID" -eq 0 ]; then
  rm -rf /cluster/logger/eventlogs/*
  /cluster/hadoop/bin/hdfs dfs -copyToLocal /eventlogs /cluster/logger/
fi

# WARNING: SparkContext.addFile copies the file into $SPARK_WORK_DIR which is being collected
# below! Using --exclude case-by-case for now (and only in COMPRESS mode).

echo  "Collecting logs $ITER"
tar_code=0
tar_name="$NODE_NAME.$DX_CLUSTER_HOSTNAME.$ITER.tar.gz"
tar -czvf "$tar_name" --warning=no-file-changed --exclude='*.jar' --exclude='*.db' \
  /cluster/logger/eventlogs \
  "$SPARK_LOG_DIR" \
  "$SPARK_WORK_DIR" \
  "$HADOOP_LOG_DIR" \
  ${ERROR_FILE} \
  ${OUT_FILE} \
  /cluster/logger/dstat.log \
  /cluster/dx-cluster.environment \
  "$SPARK_CONF_DIR" || tar_code=$?
if (( tar_code != 0 )) && (( tar_code != 1 )); then
  exit $tar_code
fi
dx upload "$tar_name" --destination "$DX_PROJECT_CONTEXT_ID:$DEST/"

if [ $ITER -ne 0 ]; then
  # Clean up previous collection from project
  # shellcheck disable=SC2004
  PREV_ITER=$(($ITER-1))
  prev_tar_name="$NODE_NAME.$DX_CLUSTER_HOSTNAME.$PREV_ITER.tar.gz"
  dx rm -f "$DX_PROJECT_CONTEXT_ID:$DEST/$prev_tar_name" || true
fi
echo  "Done."
