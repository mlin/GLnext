#!/bin/bash

set -euxo pipefail

echo "in bootstrap.sh"

. /cluster/dx-cluster.environment
. /home/dnanexus/environment

# enable temporary-workaround cluster log uploader
nohup /bin/bash /cluster/logger/log_svc.sh -i 300 -c -f GLnext_clusterlogs > /cluster/logger/log_svc.log 2>&1  &

echo "out bootstrap.sh"
