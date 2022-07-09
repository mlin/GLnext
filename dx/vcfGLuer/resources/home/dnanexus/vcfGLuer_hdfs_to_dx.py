# PySpark script to transfer pVCF (parts) from HDFS to DNAnexus output container

import pyspark
import time
import tempfile
import sys
import os
import subprocess
import dxpy
import re
import json
import shlex

# list parts under hdfs:/vcfGLuer/out/
hdfs_parts = []
ls = subprocess.run(
    [os.path.join(os.environ["HADOOP_HOME"], "bin", "hadoop"), "fs", "-ls", "/vcfGLuer/out"],
    stdout=subprocess.PIPE,
    universal_newlines=True,
    check=True,
)
part_re = re.compile("/vcfGLuer/out/[-_A-Za-z0-9\\.]+\\.bgz")
for line in ls.stdout.splitlines():
    m = part_re.search(line)
    if m:
        hdfs_parts.append(m.group(0))
assert len(hdfs_parts) > 2
assert sum(1 for part in hdfs_parts if "00HEADER" in part) == 1
header_part = next(part for part in hdfs_parts if "00HEADER" in part)
assert sum(1 for part in hdfs_parts if "zzEOF" in part) == 1
eof_part = next(part for part in hdfs_parts if "zzEOF" in part)

print(f"copying {len(hdfs_parts)} dxfiles to hdfs:/vcfGLuer/in/", file=sys.stderr)
spark = pyspark.sql.SparkSession.builder.getOrCreate()
hdfs_parts_rdd = spark.sparkContext.parallelize(
    list(part for part in hdfs_parts if part not in (header_part, eof_part)),
    max(spark.sparkContext.defaultParallelism, 64),
)

output_name = os.environ.get("output_name", "merged")
dxfolder = "/" + output_name
dx_time_accumulator = spark.sparkContext.accumulator(0.0)
hdfs_time_accumulator = spark.sparkContext.accumulator(0.0)


def process_part(hdfs_part):
    global dx_time_accumulator
    global hdfs_time_accumulator

    # As we buffer the part from HDFS to upload to DNAnexus, attach the header and EOF to make a
    # well-formed vcf.gz, also reformulating the filename appropriately.

    part_basename = os.path.basename(hdfs_part)
    prefix = "part-"
    suffix = ".bgz"
    assert part_basename.startswith(prefix) and part_basename.endswith(suffix), part_basename
    part_basename = part_basename[len(prefix) : len(part_basename) - len(suffix)]
    part_basename = output_name + "-" + part_basename + ".vcf.gz"

    with tempfile.TemporaryDirectory() as tmpdir:
        t0 = time.time()
        proc = subprocess.run(
            "$HADOOP_HOME/bin/hadoop fs -cat"
            f" {shlex.quote(header_part)} {shlex.quote(hdfs_part)}"
            f" {shlex.quote(eof_part)} > {shlex.quote(os.path.join(tmpdir,part_basename))}",
            shell=True,
            universal_newlines=True,
            stderr=subprocess.PIPE,
        )
        assert proc.returncode == 0, f"Failed copying hdfs:{hdfs_part}\n{proc.stderr}"
        t1 = time.time()
        hdfs_time_accumulator += t1 - t0

        dxfile = dxpy.upload_local_file(
            os.path.join(tmpdir, part_basename), folder=dxfolder, parents=True
        )
        dx_time_accumulator += time.time() - t1
        return dxfile.get_id()


dxfile_ids = hdfs_parts_rdd.map(process_part).collect()
assert len(dxfile_ids) == len(hdfs_parts) - 2

print(f"cumulative seconds getting from HDFS: {hdfs_time_accumulator.value}", file=sys.stderr)
print(f"cumulative seconds uploading dxfiles: {dx_time_accumulator.value}", file=sys.stderr)

# write job_output.json
with open("job_output.json.tmp", "w") as job_output:
    json.dump({"pvcf_parts": list({"$dnanexus_link": dxid} for dxid in dxfile_ids)}, job_output)
os.rename("job_output.json.tmp", "job_output.json")
