#!/bin/bash

set -euxo pipefail

echo "in prebootstrap.sh"

update-alternatives --set java /usr/lib/jvm/java-11-openjdk-amd64/bin/java

# update hdfs-site.xml to increase dfs.datanode.max.transfer.threads
find / -name hdfs-site.xml -type f || true
sed -i 's=<configuration>=<configuration><property><name>dfs.datanode.max.transfer.threads</name><value>16384</value></property>=g' /cluster/hadoop/etc/hadoop/hdfs-site.xml
cat /cluster/hadoop/etc/hadoop/hdfs-site.xml

echo "out prebootstrap.sh"
