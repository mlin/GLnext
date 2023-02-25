# PySpark script to "localize" gVCF files from DNAnexus project to the apachespark cluster's HDFS

import glob
import os
import subprocess
import sys
import tempfile
import time

import dxpy
import pyspark

# desired HDFS replication factor; perhaps higher than the dxspark apparent default of 2
DFS_REPLICATION = 3

# load gVCF manifest from in/vcf_manifest/*
dxid_list = []
for fn in glob.glob("/home/dnanexus/in/vcf_manifest/*"):
    with open(fn) as infile:
        for line in infile:
            assert "file-" in line, "manifest should contain file-xxxx IDs, one ID per line"
            dxid_list.append(line.strip())

print(f"copying {len(dxid_list)} dxfiles to hdfs:///vcfGLuer/in/", file=sys.stderr)
spark = pyspark.sql.SparkSession.builder.getOrCreate()
dxid_rdd = spark.sparkContext.parallelize(
    dxid_list, min(spark.sparkContext.defaultParallelism, 256)
)
subprocess.run("$HADOOP_HOME/bin/hadoop fs -mkdir -p /vcfGLuer/in", shell=True, check=True)

dx_time_accumulator = spark.sparkContext.accumulator(0.0)
hdfs_time_accumulator = spark.sparkContext.accumulator(0.0)


def process_dxfile(dxid):
    global dx_time_accumulator
    global hdfs_time_accumulator

    project_id = None
    dxid = dxid.split(":")
    if len(dxid) == 1:
        dxid = dxid[0]
    else:
        project_id = dxid[0]
        dxid = dxid[1]
    with tempfile.TemporaryDirectory() as tmpdir:
        t0 = time.time()
        desc = dxpy.describe(dxpy.dxlink(dxid, project_id=project_id))
        fn = desc["name"]
        dxpy.download_dxfile(
            dxid, os.path.join(tmpdir, fn), project=project_id, describe_output=desc
        )
        t1 = time.time()
        dx_time_accumulator += t1 - t0
        proc = subprocess.run(
            [
                os.path.join(os.environ["HADOOP_HOME"], "bin", "hadoop"),
                "fs",
                "-D",
                "dfs.replication=" + str(DFS_REPLICATION),
                "-put",
                "-f",
                os.path.join(tmpdir, fn),
                "/vcfGLuer/in/",
            ],
            universal_newlines=True,
            stderr=subprocess.PIPE,
        )
        assert proc.returncode == 0, f"Failed copying {dxid} to HDFS:\n{proc.stderr}"
        hdfs_time_accumulator += time.time() - t1
        return "hdfs:///vcfGLuer/in/" + fn


hdfs_paths = dxid_rdd.map(process_dxfile).collect()
assert len(hdfs_paths) == len(dxid_list)

print(f"cumulative seconds downloading dxfiles: {dx_time_accumulator.value}", file=sys.stderr)
print(f"cumulative seconds putting to HDFS: {hdfs_time_accumulator.value}", file=sys.stderr)

# write hdfs paths to vcfGLuer_in.hdfs.manifest
manifest_out = "/home/dnanexus/vcfGLuer_in.hdfs.manifest"
with open(manifest_out + ".tmp", "w") as outfile:
    for hdfs_path in hdfs_paths:
        print(hdfs_path, file=outfile)
os.rename(manifest_out + ".tmp", manifest_out)
