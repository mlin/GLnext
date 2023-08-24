# PySpark script to transfer spVCF from HDFS to DNAnexus output container

import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import time

import dxpy
import pyspark

output_name = os.environ.get("output_name", "merged")
spvcf_decode = os.environ.get("spvcf_decode", "false") == "true"

# list parts under hdfs:/vcfGLuer/out
ls = subprocess.run(
    [
        os.path.join(os.environ["HADOOP_HOME"], "bin", "hadoop"),
        "fs",
        "-ls",
        f"/vcfGLuer/out/{output_name}",
    ],
    stdout=subprocess.PIPE,
    universal_newlines=True,
    check=True,
)
hdfs_parts = []
vcf_re = re.compile(r"/vcfGLuer/.*spvcf\.gz")
success = False
for line in ls.stdout.splitlines():
    m = vcf_re.search(line)
    if m:
        hdfs_parts.append(m.group(0))
    if "_SUCCESS" in line:
        success = True
assert success and hdfs_parts

print(f"copying {len(hdfs_parts)} files to dx", file=sys.stderr)
spark = pyspark.sql.SparkSession.builder.getOrCreate()
hdfs_parts_rdd = spark.sparkContext.parallelize(
    hdfs_parts,
    min(spark.sparkContext.defaultParallelism, (500 if spvcf_decode else 100)),
)

dxfolder = "/" + output_name
dx_time_accumulator = spark.sparkContext.accumulator(0.0)
hdfs_time_accumulator = spark.sparkContext.accumulator(0.0)
spvcf_decode_time_accumulator = spark.sparkContext.accumulator(0.0)


def run_spvcf_decode(spvcf_fn):
    assert spvcf_fn.endswith("spvcf.gz")
    pvcf_fn = spvcf_fn[:-8] + "vcf.gz"
    subprocess.run(
        f"set -euo pipefail; bgzip -dc {shlex.quote(spvcf_fn)} | spvcf decode | bgzip -cl1@4 > {shlex.quote(pvcf_fn)}",
        shell=True,
        executable="/bin/bash",
        check=True,
    )
    return pvcf_fn


def process_part(hdfs_part):
    global dx_time_accumulator
    global hdfs_time_accumulator
    global spvcf_decode_time_accumulator

    with tempfile.TemporaryDirectory() as tmpdir:
        t0 = time.time()
        proc = subprocess.run(
            f"$HADOOP_HOME/bin/hadoop fs -get {shlex.quote(hdfs_part)} {shlex.quote(tmpdir)}",
            shell=True,
            universal_newlines=True,
            stderr=subprocess.PIPE,
        )
        assert proc.returncode == 0, f"Failed copying hdfs:{hdfs_part}\n{proc.stderr}"
        t1 = time.time()
        hdfs_time_accumulator += t1 - t0

        spvcf_fn = os.path.join(tmpdir, os.path.basename(hdfs_part))
        to_upload = [spvcf_fn]

        if spvcf_decode:
            to_upload.append(run_spvcf_decode(spvcf_fn))
            spvcf_decode_time_accumulator += time.time() - t1

        t2 = time.time()
        dxfile_ids = []
        for local_fn in to_upload:
            dxfile = dxpy.upload_local_file(
                local_fn,
                folder=dxfolder,
                parents=True,
                write_buffer_size=268435456,
            )
            dxfile_ids.append(dxfile.get_id())
        dx_time_accumulator += time.time() - t2

        return dxfile_ids


dxfile_ids = hdfs_parts_rdd.flatMap(process_part).collect()
assert len(dxfile_ids) == len(hdfs_parts) * (2 if spvcf_decode else 1)

print(
    f"cumulative seconds getting from HDFS: {hdfs_time_accumulator.value}",
    file=sys.stderr,
)
if spvcf_decode:
    print(
        f"cumulative seconds for spvcf decode: {spvcf_decode_time_accumulator.value}",
        file=sys.stderr,
    )
print(
    f"cumulative seconds uploading dxfiles: {dx_time_accumulator.value}",
    file=sys.stderr,
)

# write job_output.json
with open("job_output.json.tmp", "w") as job_output:
    json.dump(
        {"files": list({"$dnanexus_link": dxid} for dxid in dxfile_ids)}, job_output
    )
os.rename("job_output.json.tmp", "job_output.json")
