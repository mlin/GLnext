#!/bin/bash

export LC_ALL=C

main() {
    set -euxo pipefail

    # Set exit trap to ensure complete log delivery (see log_svc.sh)
    trap 'sleep 300 && flock -w 3600 /cluster/logger/collect_log.sh date' EXIT

    # check JAR
    java -version
    jar tf GLnext-*.jar | grep GLnext/SparkApp.class

    # check spark conf in prebootstrap.sh
    cat "$PRE_BOOTSTRAP_LOG" || echo "no PRE_BOOTSTRAP_LOG"
    grep spark.shuffle.service.enabled /cluster/spark/conf/spark-defaults.conf | grep true
    grep SPARK_DAEMON_MEMORY= /cluster/spark/conf/spark-env.sh

    # copy input gVCFs from dnanexus to hdfs
    HDFS_RETRY_CONF='--conf spark.hadoop.dfs.client.retry.policy.enabled=true --conf spark.hadoop.dfs.client.retry.window.base=5000 --conf spark.hadoop.dfs.client.max.block.acquire.failures=5'
    dx-download-all-inputs
    dx-spark-submit --log-level WARN \
        --conf spark.driver.maxResultSize=0 \
        --conf spark.default.parallelism=$spark_default_parallelism \
        $HDFS_RETRY_CONF \
        GLnext_dx_to_hdfs.py || true
    # Spark occasionally throws some meaningless exception during shutdown of a successful app,
    # so ignore its exit code and check for sentinel file our script created atomically on success.
    if ! [[ -f GLnext_in.hdfs.manifest ]]; then
        exit 1
    fi

    # checking HDFS replication:
    # shuf GLnext_in.hdfs.manifest | head -n 10 | xargs -i -n1 hadoop fs -stat %r {}

    # run GLnext
    # references for gnarly GC tuning:
    #   https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/
    #   https://www.oracle.com/technical-resources/articles/java/g1gc.html
    #   https://databricks.com/blog/2015/05/28/tuning-java-garbage-collection-for-spark-applications.html
    #   https://www.slideshare.net/databricks/tuning-apache-spark-for-largescale-workloads-gaoxiang-liu-and-sital-kedia
    #   https://aws.amazon.com/blogs/big-data/spark-enhancements-for-elasticity-and-resiliency-on-amazon-emr/
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
    # NOTE: other obscure spark tuning settings are set in prebootstrap.sh
    dx-spark-submit --log-level WARN \
        --executor-cores 11 \
        --conf spark.driver.defaultJavaOptions="$all_java_options" \
        --conf spark.executor.defaultJavaOptions="$all_java_options" \
        --conf spark.executor.memory=68g \
        --conf spark.memory.fraction=0.5 \
        --conf spark.memory.storageFraction=0.4 \
        --conf spark.reducer.maxReqsInFlight=8 \
        --conf spark.driver.maxResultSize=0 \
        --conf spark.task.maxFailures=3 \
        --conf spark.stage.maxConsecutiveAttempts=5 \
        --conf spark.speculation=true \
        --conf spark.speculation.interval=1m \
        --conf spark.speculation.multiplier=8 \
        --conf spark.speculation.quantile=0.98 \
        --conf spark.speculation.minTaskRuntime=30m \
        --conf spark.default.parallelism="$spark_default_parallelism" \
        --conf spark.sql.shuffle.partitions="$spark_default_parallelism" \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.initialPartitionNum="$spark_default_parallelism" \
        --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false \
        $HDFS_RETRY_CONF \
        --name GLnext GLnext-*.jar \
        --manifest --config "$config" \
        $filter_bed_arg $filter_contigs_arg $split_bed_arg \
        GLnext_in.hdfs.manifest "hdfs:///GLnext/out/$output_name" \
        || true
    # check for _SUCCESS sentinel output file
    if ! "$HADOOP_HOME/bin/hadoop" fs -get "/GLnext/out/$output_name/_SUCCESS" . ; then
        exit 1
    fi

    # upload pVCF parts from hdfs to dnanexus
    rm -f job_output.json
    dx-spark-submit --log-level WARN \
        --conf spark.driver.maxResultSize=0 \
        --conf spark.default.parallelism="$spark_default_parallelism" \
        $HDFS_RETRY_CONF \
        GLnext_hdfs_to_dx.py || true
    if ! [[ -f job_output.json ]]; then
        exit 1
    fi
}
