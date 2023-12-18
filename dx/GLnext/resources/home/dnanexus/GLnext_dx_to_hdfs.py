# PySpark script to "localize" gVCF files from DNAnexus project to the apachespark
# cluster's HDFS

import glob
import os
import random
import shlex
import subprocess
import sys
import tempfile
import time

import dxpy
import pyspark

ref_genome = glob.glob("/home/dnanexus/in/ref_genome/*")
gvcf_norm = len(ref_genome) > 0
if gvcf_norm:
    ref_genome = ref_genome[0]
multiply = os.environ.get("_multiply", None)
if multiply:
    multiply = int(multiply)
else:
    multiply = 1
DFS_REPLICATION = 2

# load gVCF manifest from in/vcf_manifest/*
dxid_list = []
for fn in glob.glob("/home/dnanexus/in/vcf_manifest/*"):
    with open(fn) as infile:
        for line in infile:
            assert (
                "file-" in line
            ), "manifest should contain file-xxxx IDs, one ID per line"
            dxid_list.append(line.strip())

print(
    f"copying {len(dxid_list)}*{multiply} dxfiles to hdfs:///GLnext/in/",
    file=sys.stderr,
)
spark = pyspark.sql.SparkSession.builder.getOrCreate()
spark_ctx = spark.sparkContext
dxid_rdd = spark_ctx.parallelize(
    dxid_list,
    min(spark_ctx.defaultParallelism, 1024 * multiply * (4 if gvcf_norm else 1)),
)
# create 256 subdirectories because of 1M files per directory limit
hdfs_dirs = [f"/GLnext/in/{subd:02x}" for subd in range(0xFF)]
for hdfs_dir in hdfs_dirs:
    subprocess.run(
        "$HADOOP_HOME/bin/hadoop fs -mkdir -p " + hdfs_dir, shell=True, check=True
    )

if gvcf_norm:
    # prepare gvcf_norm reference genome and broadcast to all nodes
    assert os.path.isfile(ref_genome)

    refdir = os.path.join(os.getcwd(), "ref_genome.unpack")
    subprocess.run(
        f"unpack_fasta_dir.sh {shlex.quote(ref_genome)} {shlex.quote(refdir)}",
        shell=True,
        check=True,
    )
    assert os.path.isdir(refdir)
    with open(os.path.join(refdir, "_FOO"), "w") as _:
        pass
    for fn in glob.glob(refdir + "/*"):
        spark_ctx.addFile(fn)


dx_time_accumulator = spark_ctx.accumulator(0.0)
gvcf_norm_time_accumulator = spark_ctx.accumulator(0.0)
hdfs_time_accumulator = spark_ctx.accumulator(0.0)


def process_dxfile(dxid):
    global dx_time_accumulator
    global gvcf_norm_time_accumulator
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
        fn = os.path.join(tmpdir, desc["name"])
        dxpy.download_dxfile(dxid, fn, project=project_id, describe_output=desc)
        t1 = time.time()
        dx_time_accumulator += t1 - t0
        if gvcf_norm:
            fn = run_gvcf_norm(fn)
            gvcf_norm_time_accumulator += time.time() - t1
            t1 = time.time()
        ans = []
        for i in range(multiply):
            hdfs_dir = random.choice(hdfs_dirs)
            prefix = f"_x{i+1}_" if multiply > 1 else ""
            dest = f"{hdfs_dir}/{prefix}{os.path.basename(fn)}"
            hdfs_put(fn, dest)
            ans.append("hdfs://" + dest)
        hdfs_time_accumulator += time.time() - t1
        return ans


def run_gvcf_norm(gvcf_fn):
    ref_genome_dir = os.path.dirname(pyspark.SparkFiles.get("_FOO"))
    out_fn = os.path.join(os.path.dirname(gvcf_fn), "norm_" + os.path.basename(gvcf_fn))
    try:
        subprocess.run(
            f"set -euo pipefail; bgzip -dc {shlex.quote(gvcf_fn)} | gvcf_norm -r {ref_genome_dir} - | bgzip -cl1@4 > {shlex.quote(out_fn)}",
            shell=True,
            executable="/bin/bash",
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError("gvcf_norm failed: " + e.stderr.strip())
    return out_fn


def hdfs_put(local_path, hdfs_path):
    proc = subprocess.run(
        [
            os.path.join(os.environ["HADOOP_HOME"], "bin", "hadoop"),
            "fs",
            "-D",
            "dfs.replication=" + str(DFS_REPLICATION),
            "-put",
            "-f",
            local_path,
            hdfs_path,
        ],
        universal_newlines=True,
        stderr=subprocess.PIPE,
    )
    assert proc.returncode == 0, f"Failed copying {local_path} to HDFS:\n{proc.stderr}"


hdfs_paths = dxid_rdd.flatMap(process_dxfile).collect()
assert len(hdfs_paths) == len(dxid_list) * multiply

print(
    f"cumulative seconds downloading dxfiles: {dx_time_accumulator.value}",
    file=sys.stderr,
)
if gvcf_norm:
    print(
        f"cumulative seconds for gvcf_norm: {gvcf_norm_time_accumulator.value}",
        file=sys.stderr,
    )
print(
    f"cumulative seconds putting to HDFS: {hdfs_time_accumulator.value}",
    file=sys.stderr,
)

# write hdfs paths to GLnext_in.hdfs.manifest
manifest_out = "/home/dnanexus/GLnext_in.hdfs.manifest"
with open(manifest_out + ".tmp", "w") as outfile:
    for hdfs_path in hdfs_paths:
        print(hdfs_path, file=outfile)
os.rename(manifest_out + ".tmp", manifest_out)
