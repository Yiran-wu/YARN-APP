#!/usr/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"

function printUsage {
  echo "Usage: submit.sh <numWorkers> <pathHdfs> "
  echo -e "  numWorkers        \tNumber of workers to launch"
  echo -e "  pathHdfs          \tPath on HDFS to put jar and distribute it to YARN"
  echo
  echo "Example: ./submit.sh 10 hdfs://cluster/tmp/ "
}

if [[ "$#" -ne 2 ]] ; then
  printUsage
  exit 1
fi

if [[ -z "$HADOOP_HOME" ]]; then
  echo "\$HADOOP_HOME is unset, please set this variable to connect to HDFS and YARN" >&2
  exit 1
else
  echo "Using \$HADOOP_HOME set to '$HADOOP_HOME'"
fi

if [[ -z "$YARN_HOME" ]]; then
  echo "\$YARN_HOME is unset, will use \$HADOOP_HOME instead."
fi

YARN_HOME=${YARN_HOME:-${HADOOP_HOME}}


NUM_WORKERS=$1
HDFS_PATH=$2

echo "Uploading files to HDFS to distribute runtime"
JAR_LOCAL=${SCRIPT_DIR}/../target/iwantfind-1.0-SNAPSHOT.jar
${HADOOP_HOME}/bin/hadoop fs -mkdir -p ${HDFS_PATH}
${HADOOP_HOME}/bin/hadoop fs -put -f ${JAR_LOCAL} ${HDFS_PATH}/iwantfind-1.0-SNAPSHOT.jar
${HADOOP_HOME}/bin/hadoop fs -put -f ${SCRIPT_DIR}/run.sh ${HDFS_PATH}/run.sh
${HADOOP_HOME}/bin/hadoop fs -put -f ${SCRIPT_DIR}/log4j.properties ${HDFS_PATH}/log4j.properties
${YARN_HOME}/bin/yarn jar ${JAR_LOCAL} com.iwantfind.Client \
    -num_workers $NUM_WORKERS \
    -resource_path ${HDFS_PATH}