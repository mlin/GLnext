# PySpark script to transfer pVCF files from HDFS to DNAnexus output container

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
vcf_re = re.compile(r"/vcfGLuer/.*\.vcf\.gz")
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
    min(spark.sparkContext.defaultParallelism, 256),
)

dxfolder = "/" + output_name
dx_time_accumulator = spark.sparkContext.accumulator(0.0)
hdfs_time_accumulator = spark.sparkContext.accumulator(0.0)


def process_part(hdfs_part):
    global dx_time_accumulator
    global hdfs_time_accumulator

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

        dxfile = dxpy.upload_local_file(
            os.path.join(tmpdir, os.path.basename(hdfs_part)),
            folder=dxfolder,
            parents=True,
            write_buffer_size=1073741824,
        )
        dx_time_accumulator += time.time() - t1
        return dxfile.get_id()


dxfile_ids = hdfs_parts_rdd.map(process_part).collect()
assert len(dxfile_ids) == len(hdfs_parts)

print(f"cumulative seconds getting from HDFS: {hdfs_time_accumulator.value}", file=sys.stderr)
print(f"cumulative seconds uploading dxfiles: {dx_time_accumulator.value}", file=sys.stderr)

# write job_output.json
with open("job_output.json.tmp", "w") as job_output:
    json.dump({"pvcf_parts": list({"$dnanexus_link": dxid} for dxid in dxfile_ids)}, job_output)
os.rename("job_output.json.tmp", "job_output.json")
