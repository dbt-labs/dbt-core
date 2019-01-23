#!/bin/sh

service ssh start
source ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh
${HADOOP_HOME}/sbin/start-dfs.sh
${HADOOP_HOME}sbin/start-yarn.sh

# idk
while true; do sleep 100; done
