#!/bin/bash

set -euxo pipefail

echo "in prebootstrap.sh"

update-alternatives --set java /usr/lib/jvm/java-11-openjdk-amd64/bin/java

mv /cluster/log_collector.sh /cluster/log_collector.sh.old
mv /cluster/log_collector.sh.new /cluster/log_collector.sh

echo "out prebootstrap.sh"
