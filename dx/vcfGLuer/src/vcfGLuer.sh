#!/bin/bash

export LC_ALL=C

main() {
    set -euxo pipefail

    cat $PRE_BOOTSTRAP_LOG || echo "no PRE_BOOTSTRAP_LOG"
    java -version

    dx-download-all-inputs

    HDFS_RETRY_CONF='--conf spark.hadoop.dfs.client.retry.policy.enabled=true --conf spark.hadoop.dfs.client.retry.window.base=5000 --conf spark.hadoop.dfs.client.max.block.acquire.failures=5'

    # copy input gVCFs from dnanexus to hdfs
    dx-spark-submit --log-level WARN \
        --conf spark.driver.maxResultSize=0 \
        --conf spark.default.parallelism=$spark_default_parallelism \
        $HDFS_RETRY_CONF \
        vcfGLuer_dx_to_hdfs.py || true
    # Spark occasionally throws some meaningless exception during shutdown of a successful app,
    # so ignore its exit code and check for sentinel file our script created atomically on success.
    if ! [[ -f vcfGLuer_in.hdfs.manifest ]]; then
        exit 1
    fi

    # checking HDFS replication:
    # shuf vcfGLuer_in.hdfs.manifest | head -n 10 | xargs -i -n1 hadoop fs -stat %r {}

    # run vcfGLuer
    # references for gnarly GC tuning:
    #   https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
    #   https://www.oracle.com/technical-resources/articles/java/g1gc.html
    #   https://databricks.com/blog/2015/05/28/tuning-java-garbage-collection-for-spark-applications.html
    all_java_options="\
    -Xss16m -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxGCPauseMillis=1000 \
    -XX:+PrintFlagsFinal \
    $java_options"
    filter_bed_arg=""
    if [[ -n ${filter_bed:-} ]]; then
        filter_bed_arg="--filter-bed $(find ./in/filter_bed -type f)"
    fi
    filter_contigs_arg=""
    if [[ -n ${filter_contigs:-} ]]; then
        filter_contigs_arg="--filter-contigs $filter_contigs"
    fi
    split_bed_arg=""
    if [[ -n ${split_bed:-} ]]; then
        split_bed_arg="--split-bed $(find ./in/split_bed -type f)"
    fi
    dx-spark-submit --log-level WARN --collect-logs \
        --conf spark.driver.defaultJavaOptions="$all_java_options" \
        --conf spark.executor.defaultJavaOptions="$all_java_options" \
        --conf spark.executor.memory=72g \
        --conf spark.driver.maxResultSize=0 \
        --conf spark.task.maxFailures=3 \
        --conf spark.stage.maxConsecutiveAttempts=2 \
        --conf spark.speculation=true \
        --conf spark.speculation.interval=1m \
        --conf spark.speculation.multiplier=8 \
        --conf spark.speculation.quantile=0.98 \
        --conf spark.speculation.minTaskRuntime=30m \
        --conf spark.default.parallelism=$spark_default_parallelism \
        --conf spark.sql.shuffle.partitions=$spark_default_parallelism \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.initialPartitionNum=$spark_default_parallelism \
        --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false \
        $HDFS_RETRY_CONF \
        --name vcfGLuer vcfGLuer-*.jar \
        --manifest --tmp-dir hdfs:///tmp --config $config $filter_bed_arg $filter_contigs_arg \
        vcfGLuer_in.hdfs.manifest "hdfs:///vcfGLuer/out/$output_name" \
        || true
    $HADOOP_HOME/bin/hadoop fs -get "/vcfGLuer/out/$output_name/_SUCCESS" .

    # upload pVCF parts from hdfs to dnanexus
    rm -f job_output.json
    dx-spark-submit --log-level WARN \
        --conf spark.driver.maxResultSize=0 \
        --conf spark.default.parallelism=$spark_default_parallelism \
        $HDFS_RETRY_CONF \
        vcfGLuer_hdfs_to_dx.py || true
    if ! [[ -f job_output.json ]]; then
        exit 1
    fi
}
