#!/usr/bin/env bash

set -e

SMV_TOOLS="$(cd "`dirname "$0"`"; pwd)"

export PYSPARK_DRIVER_PYTHON="$(which jupyter)"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --FileContentsManager.root_dir=notebooks --NotebookApp.open_browser=False"

# Pass through the options from `smv-jupyter` invocation through to `smv-pyshell`
# This will allow the user to specify pyspark options like:
# `smv-jupyter -- --master=yarn-client --num-executors=10`
# `smv-jupyter -- --conf="spark.driver.maxResultSize=0"`

PROJ_TOOLS="$(cd "`dirname "$0"`"; pwd)"

# Build the RE lib package so that it can be shipped to worker nodes
# python "${PROJ_TOOLS}/createLibPkg.py"

smv-pyshell \
    -- \
    --executor-memory 16g --driver-memory 8g \
    --conf spark.yarn.queue=SpkgSonar \
    --conf spark.dynamicAllocation.maxExecutors=2 \
    --conf spark.yarn.executor.memoryOverhead=4048 \
    --conf spark.executor.cores=4 \
    --master yarn-client

#rm -f relib.zip
