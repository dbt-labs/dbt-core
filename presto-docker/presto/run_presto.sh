#!/bin/sh

export PATH="$PATH:${HADOOP_HOME}/bin"
hadoop fs -mkdir -p /tmp
hadoop fs -mkdir -p /user/hive/warehouse
hadoop fs -chmod g+w /tmp
hadoop fs -chmod g+w /user/hive/warehouse

cd $PRESTO_HOME/bin
./launcher run
