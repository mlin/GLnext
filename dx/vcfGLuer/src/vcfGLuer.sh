#!/bin/bash

# Spark configuration
export spark_worker_memory_fraction=0.75
export spark_driver_memory_fraction=0.1
export _JAVA_OPTIONS='
    -XX:+AlwaysPreTouch
    -Xss16m
    -Dspark.sql.shuffle.partitions=256
    -Dspark.memory.fraction=0.6
    -Dspark.executor.heartbeatInterval=300s
    -Dspark.network.timeout=900s
    -Dspark.worker.timeout=900
    -Dspark.sql.broadcastTimeout=900
    -Dspark.driver.maxResultSize=0
'
export SPARK_HOME=/spark
export SPARK_LOCAL_IP=127.0.0.1

main() {
    set -euo pipefail

    # log detailed utilization
    dstat -cmdn -N lo,eth0 60 &
    bash -c 'while true; do df -h /tmp; sleep 300; done' &

    dx-download-all-inputs
    # process dxid manifests under in/vcf_manifest/
    mkdir /tmp/vcf_in
    find in/vcf_manifest -type f -execdir cat {} + \
        | parallel --jobs 50% --delay 4 --verbose \
            bash -o pipefail -c "'dx cat {} | bgzip -dc@ 4 | vcf_line_splitter -threads 4 -MB 4096 -quiet /tmp/vcf_in/{}- > /dev/null'"
    part_count=$(find /tmp/vcf_in -type f | tee vcf_in.manifest | wc -l)

    # start spark
    numa_nodes=$(detect_numa_nodes)
    SPARK_EXECUTOR_MEMORY=$(awk '/MemTotal/ {printf("%.0f",$2*'"$spark_worker_memory_fraction"'/'"$numa_nodes"')}' /proc/meminfo)k
    SPARK_DRIVER_MEMORY=$(awk '/MemTotal/ {printf("%.0f",$2*'"$spark_driver_memory_fraction"')}' /proc/meminfo)k
    mkdir /tmp/spark_logs
    SPARK_LOG_DIR=/tmp/spark_logs
    export numa_nodes SPARK_EXECUTOR_MEMORY SPARK_DRIVER_MEMORY SPARK_LOG_DIR
    start_spark

    # run vcfGLuer
    "${SPARK_HOME}/bin/spark-submit" \
        --master 'spark://127.0.0.1:7077' --driver-memory "$SPARK_DRIVER_MEMORY" \
        --name vcfGLuer --class vcfGLuer vcfGLuer-*.jar \
        --manifest --bin-size "$genomic_range_bin_size" --delete-input-vcfs \
        vcf_in.manifest /tmp/pvcf_out \
        || true
    # Spark occasionally throws some meaningless exception during shutdown of a successful app,
    # so ignore its exit code and check for _EOF.bgz which vcfGLuer writes atomically on success.
    if ! [[ -f /tmp/pvcf_out/_EOF.bgz ]]; then
        exit 1
    fi

    # concat & upload the merged pVCF
    (cat /tmp/pvcf_out/_HEADER.bgz /tmp/pvcf_out/part-*.bgz /tmp/pvcf_out/_EOF.bgz) \
        | dx upload --brief --buffer-size 1073741824 --destination "${output_name}.vcf.gz" - \
        | xargs dx-jobutil-add-output pvcf_gz
}

detect_numa_nodes() {
    if ! numactl --hardware > /dev/null ; then
        echo "error detecting NUMA layout" >&2
        exit 1
    fi
    numactl --hardware | grep ^available | awk '{print $2}'
}

start_spark() {
    # fetch & extract spark-3.1.2-bin-hadoop3.2.tgz
    # TODO: make asset
    mkdir -p "$SPARK_HOME"
    dx cat file-G6BFy90076vpyG455X349YzZ | tar zx -C "$SPARK_HOME" --strip-components 1
    # NOTE: ../resources/usr/lib/libhadoop.so.1.0.0 is extracted from hadoop binary distro.
    # It provides Spark with fast, native-code compression routines.

    # start Spark master
    "${SPARK_HOME}/sbin/start-master.sh" --host 127.0.0.1 >&2
    echo "started Spark master at spark://127.0.0.1:7077" >&2

    # start Spark workers, 1:1 binding to NUMA nodes
    threads_per_numa_node=$(( $(nproc) / numa_nodes ))
    for ((i=0; i<numa_nodes; i++)); do
        numactl --cpunodebind "$i" --membind="$i" \
            "${SPARK_HOME}/sbin/spark-daemon.sh" start org.apache.spark.deploy.worker.Worker "$(( i + 1 ))" \
                --webui-port "$(( 8081 + i ))" spark://127.0.0.1:7077 --host 127.0.0.1 \
                --cores "$threads_per_numa_node" --memory "$SPARK_EXECUTOR_MEMORY" >&2
    done
    echo "started $numa_nodes Spark worker(s) each with $threads_per_numa_node threads and $SPARK_EXECUTOR_MEMORY memory" >&2
}
