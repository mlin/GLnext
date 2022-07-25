#!/bin/bash

export LC_ALL=C

main() {
    set -euxo pipefail

    dx-download-all-inputs

    # copy input gVCFs from dnanexus to hdfs
    dx-spark-submit --log-level WARN \
        --conf spark.driver.maxResultSize=0 \
        --conf spark.default.parallelism=$spark_default_parallelism \
        vcfGLuer_dx_to_hdfs.py || true
    # Spark occasionally throws some meaningless exception during shutdown of a successful app,
    # so ignore its exit code and check for sentinel file our script created atomically on success.
    if ! [[ -f vcfGLuer_in.hdfs.manifest ]]; then
        exit 1
    fi

    # run vcfGLuer
    # references for gnarly GC tuning:
    #   https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
    #   https://www.oracle.com/technical-resources/articles/java/g1gc.html
    #   https://databricks.com/blog/2015/05/28/tuning-java-garbage-collection-for-spark-applications.html
    # dxspark JVM 8 notes:
    #   - mixed experience with G1GC
    #   - UseLargePages gets into fragmentation trouble (?) on larger instance types, potentially
    #     triggering *kernel oom-killer* (vs. JVM OutOfMemory)
    # -XX:+UseParallelGC -XX:GCTimeRatio=19 \
    # -XX:+UseG1GC -XX:MaxGCPauseMillis=500 -XX:ParallelGCThreads=8 -XX:ConcGCThreads=8 -XX:InitiatingHeapOccupancyPercent=35 \
    # -XX:+UseLargePages \
    all_java_options="\
    -Xss16m \
    -XX:+UseG1GC -XX:MaxGCPauseMillis=500 -XX:ParallelGCThreads=8 -XX:ConcGCThreads=8 -XX:InitiatingHeapOccupancyPercent=35 \
    -XX:+PrintFlagsFinal \
    $java_options"
    #    --conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=$spark_default_parallelism \
    #    --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false \
    #    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=512m \
    #
    #    --conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=$spark_default_parallelism \
    #    --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=true \
    #    --conf spark.sql.adaptive.minPartitionSize=16m \
    #    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=80m \
    dx-spark-submit --log-level WARN --collect-logs \
        --conf spark.driver.defaultJavaOptions="$all_java_options" \
        --conf spark.executor.defaultJavaOptions="$all_java_options" \
        --conf spark.driver.maxResultSize=0 \
        --conf spark.default.parallelism=$spark_default_parallelism \
        --conf spark.sql.shuffle.partitions=$spark_default_parallelism \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=$spark_default_parallelism \
        --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false \
        --name vcfGLuer --class vcfGLuer vcfGLuer-*.jar \
        --manifest --delete-input-vcfs --config $config \
        vcfGLuer_in.hdfs.manifest hdfs:///vcfGLuer/out \
        || true
    $HADOOP_HOME/bin/hadoop fs -ls /vcfGLuer/out > ls.txt
    head -n 100 ls.txt
    wc -l ls.txt
    $HADOOP_HOME/bin/hadoop fs -get /vcfGLuer/out/zzEOF.bgz .

    # upload pVCF parts from hdfs to dnanexus
    rm -f job_output.json
    dx-spark-submit --log-level WARN \
        --conf spark.driver.maxResultSize=0 \
        --conf spark.default.parallelism=$spark_default_parallelism \
        vcfGLuer_hdfs_to_dx.py || true
    if ! [[ -f job_output.json ]]; then
        exit 1
    fi
}
