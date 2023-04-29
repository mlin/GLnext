#!/bin/bash

set -euxo pipefail

echo "in prebootstrap.sh"

# set default JVM to java 11 (installation of which is specified in dxapp.json)
update-alternatives --set java /usr/lib/jvm/java-11-openjdk-amd64/bin/java

# update hdfs-site.xml to increase dfs.datanode.max.transfer.threads
find / -name hdfs-site.xml -type f || true
sed -i 's=<configuration>=<configuration><property><name>dfs.datanode.max.transfer.threads</name><value>4096</value></property><property><name>dfs.datanode.handler.count</name><value>16</value></property>=g' /cluster/hadoop/etc/hadoop/hdfs-site.xml
cat /cluster/hadoop/etc/hadoop/hdfs-site.xml

# add low-level spark tuning settings to spark-defaults.conf
# (others are set with spark-submit, but at least some of these must be set
#  prior to worker startup)
find / -name spark-defaults.conf -type f || true
echo '

spark.network.timeout                         300s
spark.executor.heartbeatInterval              60s
spark.shuffle.io.maxRetries                   8
spark.shuffle.io.retryWait                    10s
spark.shuffle.io.backLog                      4096
spark.shuffle.file.buffer                     1m
spark.unsafe.sorter.spill.reader.buffer.size  1m
spark.shuffle.service.enabled                 true
spark.shuffle.service.index.cache.size        2047m
spark.local.dir                               /tmp
' >> /cluster/spark/conf/spark-defaults.conf
cat /cluster/spark/conf/spark-defaults.conf

echo '
SPARK_DAEMON_MEMORY=8g
' >> /cluster/spark/conf/spark-env.sh
cat /cluster/spark/conf/spark-env.sh

echo "out prebootstrap.sh"
