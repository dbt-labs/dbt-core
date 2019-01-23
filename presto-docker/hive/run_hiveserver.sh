#!/bin/sh
hadoop fs -mkdir -p /tmp
hadoop fs -mkdir -p /user/hive/warehouse
hadoop fs -chmod g+w /tmp
hadoop fs -chmod g+w /user/hive/warehouse

hiveserver2 --hiveconf hive.server2.enable.doAs=false
