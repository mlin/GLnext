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
    #   G1GC and UseLargePages both seem to get into trouble with the heap sizes typical on larger
    #   I3 instances (>64G). G1GC tends to hit JVM OutOfMemory exceptions, while UseLargePages can
    #   trigger kernel oom-killer (fragmentation?). These recommended features should be more
    #   mature in newer JVM versions, which we'll hopefully get to try.
    #   ParallelGC (the default on Java 8) seems solid albeit "only" up to 12-16 vCPUs.
    #
    # -XX:+UseParallelGC -XX:GCTimeRatio=19 \
    # -XX:+UseG1GC -XX:MaxGCPauseMillis=500 -XX:ParallelGCThreads=8 -XX:ConcGCThreads=8 -XX:InitiatingHeapOccupancyPercent=35 \
    # -XX:+UseLargePages \
    all_java_options="\
    -Xss16m \
    -XX:+UseParallelGC -XX:GCTimeRatio=19 \
    -XX:+PrintFlagsFinal \
    $java_options"
    filter_bed_arg=""
    if [[ -n $filter_bed ]]; then
        filter_bed_arg="--filter-bed $(find ./in/filter_bed -type f)"
    fi
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
        --manifest --delete-input-vcfs --config $config $filter_bed_arg \
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
