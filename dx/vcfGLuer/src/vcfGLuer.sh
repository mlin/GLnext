#!/bin/bash

export LC_ALL=C

main() {
    set -euxo pipefail

    dx-download-all-inputs

    # copy input gVCFs from dnanexus to hdfs
    dx-spark-submit --log-level INFO --collect-logs vcfGLuer_dx_to_hdfs.py || true
    if ! [[ -f vcfGLuer_in.hdfs.manifest ]]; then
        exit 1
    fi

    # run vcfGLuer
    dx-spark-submit --log-level INFO --collect-logs \
        --name vcfGLuer --class vcfGLuer vcfGLuer-*.jar \
        --manifest --delete-input-vcfs $flags \
        vcfGLuer_in.hdfs.manifest hdfs:/vcfGLuer/out \
        || true
    $HADOOP_HOME/bin/hadoop fs -ls /vcfGLuer/out
    # Spark occasionally throws some meaningless exception during shutdown of a successful app,
    # so ignore its exit code and check for _EOF.bgz which vcfGLuer writes atomically on success.
    if ! [[ -f /tmp/pvcf_out/zzEOF.bgz ]]; then
        exit 1
    fi

    # TODO: upload pVCF parts from hdfs to dnanexus
}
