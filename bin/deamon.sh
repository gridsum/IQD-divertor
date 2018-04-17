#!/usr/bin/env bash
#
#/**
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# show usage
if [ ! -n "$1" ] ;then
    echo "usage: %s start|stop"
    exit 1
fi

# init environment variable
# CLASSPATH
CLASSPATH=""
for f in $IQD_DIVERTOR_HOME/lib/*.jar $IQD_DIVERTOR_HOME/lib/cloudera-impala2.5.34/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# JAVA OPTS
JAVA_OPTS=" -server -Xmx512m -Xms512m  -Xss256K -XX:PermSize=128M -XX:MaxPermSize=128M  -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC  -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:+CMSParallelRemarkEnabled -XX:+UseCMSCompactAtFullCollection -XX:+UseFastAccessorMethods -XX:+UseCMSInitiatingOccupancyOnly -XX:+UseCompressedOops -XX:CMSInitiatingOccupancyFraction=70 -XX:+HeapDumpOnOutOfMemoryError -XX:SurvivorRatio=8 "
# CONFIG PATH
CONFIG_PATH=$IQD_DIVERTOR_HOME/conf/
# LOG PATH
LOG_PATH=$IQD_DIVERTOR_HOME/logs/
# MAIN CLASS
MAIN_CLASS=com.gridsum.datadivertor.ScheduledExecutor

# start and stop ScheduledExecutor
if [ $1 = "start" ]; then
  nohup java -Dconfig.path=$CONFIG_PATH -Dlog.path=$LOG_PATH -classpath "$CLASSPATH" $MAIN_CLASS > /dev/null 2>&1 &
  echo "LOG_PATH: "$LOG_PATH
elif [ $1 = "stop" ]; then
  pids=`jps | grep ScheduledExecutor | awk '{print $1}'`
  for pid in $pids
    do
      kill -9 $pid
      echo "stop successfully"
    done
else
  echo "unknown command: "$1
fi

exit 0