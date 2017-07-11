#!/bin/bash

# Spark/Yarn configuration parameters.
EXECUTOR_MEMORY="16G"
EXECUTOR_CORES=10
MEMORY_OVERHEAD=4096
MAX_EXECUTORS=8
DRIVER_MEMORY="8G"
YARN_QUEUE="SpkgSonar"
#DATA_DIR="hdfs:///applications/gidr/processing/stg/spark/lung/data"
#HIVE_SCHEMA="gidr_stg_p."
#export HIVE_SCHEMA

# ADD SMV dir to path when run from deployment script
if [ -d SMV ]; then
  export PATH="`pwd`/SMV/tools:${PATH}"
fi

NOWTIME="$(date '+%Y-%m-%d-%H%M%S')"
LOGFILE="log.${NOWTIME}"
VERSION="magbc.${NOWTIME}"

# Clean up data dir for intermidiate data storage
#hdfs dfs -rm -R -skipTrash "${DATA_DIR}/*"

PROJ_TOOLS="$(cd "`dirname "$0"`"; pwd)"

# Build the RE lib package so that it can be shipped to worker nodes
#python "${PROJ_TOOLS}/createLibPkg.py"

#smv-pyrun --publish ${VERSION} -s \
#  magbc.etl \

smv-pyrun --publish ${VERSION} -m \
  magbc.sha.byhsa.LotByHsaPivotCohort \
  -- \
  --master yarn-client \
  --executor-memory ${EXECUTOR_MEMORY} \
  --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} \
  --driver-memory ${DRIVER_MEMORY} \
  --conf spark.yarn.queue=${YARN_QUEUE} \
  --conf spark.executor.cores=${EXECUTOR_CORES} \
  --conf spark.yarn.executor.memoryOverhead=${MEMORY_OVERHEAD} \
  2>&1 | tee $LOGFILE
