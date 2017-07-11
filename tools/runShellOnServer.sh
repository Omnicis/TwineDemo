#!/usr/bin/env bash
set -e

PROJ_TOOLS="$(cd "`dirname "$0"`"; pwd)"

# Build the RE lib package so that it can be shipped to worker nodes
# python "${PROJ_TOOLS}/createLibPkg.py"

smv-pyshell \
    -- \
    --executor-memory 16g --driver-memory 8g \
    --conf spark.yarn.queue=SpkgSonar \
    --conf spark.dynamicAllocation.maxExecutors=4 \
    --conf spark.yarn.executor.memoryOverhead=4048 \
    --conf spark.executor.cores=10 \
    --master yarn-client

#rm -f relib.zip
