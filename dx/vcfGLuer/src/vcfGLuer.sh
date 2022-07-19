#!/bin/bash

export LC_ALL=C

main() {
    export _JAVA_OPTIONS="$_JAVA_OPTIONS -Dspark.default.parallelism=$spark_default_parallelism -Dspark.sql.adaptive.enabled=true -Dspark.sql.adaptive.coalescePartitions.enabled=true -Dspark.sql.adaptive.coalescePartitions.parallelismFirst=false -Dspark.sql.adaptive.coalescePartitions.initialPartitionNum=$spark_default_parallelism -Xss16m $java_options"

    set -euxo pipefail

    dx-download-all-inputs

    # copy input gVCFs from dnanexus to hdfs
    dx-spark-submit --log-level WARN vcfGLuer_dx_to_hdfs.py || true
    # Spark occasionally throws some meaningless exception during shutdown of a successful app,
    # so ignore its exit code and check for _EOF.bgz which vcfGLuer writes atomically on success.
    if ! [[ -f vcfGLuer_in.hdfs.manifest ]]; then
        exit 1
    fi

    # run vcfGLuer
    dx-spark-submit --log-level WARN --collect-logs \
        --name vcfGLuer --class vcfGLuer vcfGLuer-*.jar \
        --manifest --delete-input-vcfs --config $config \
        vcfGLuer_in.hdfs.manifest hdfs:///vcfGLuer/out \
        || true
    $HADOOP_HOME/bin/hadoop fs -ls /vcfGLuer/out
    $HADOOP_HOME/bin/hadoop fs -get /vcfGLuer/out/zzEOF.bgz .

    # upload pVCF parts from hdfs to dnanexus
    rm -f job_output.json
    dx-spark-submit --log-level WARN vcfGLuer_hdfs_to_dx.py || true
    if ! [[ -f job_output.json ]]; then
        exit 1
    fi
}
