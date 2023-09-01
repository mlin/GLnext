# vcfGLuer on DNAnexus

This [DNAnexus Spark App](https://documentation.dnanexus.com/developer/apps/developing-spark-apps) for vcfGLuer handles large numbers of input files with various Spark optimizations and extra features. Usage:

### Prepare gVCF manifest file

Create a text file in your project, containing the project-x:file-y IDs of the desired gVCF files, one ID per line. Example invocation used to create the dv1KGPWGS manifest:

```bash
dx find data --folder /dv1KGPWGS --name "*.g.vcf.gz" --brief \
	| dx upload --destination dv1KGPWGS.manifest -
```

### Run vcfGLuer App

```bash
dx run vcfGLuer \
  -i vcf_manifest=dv1KGPWGS.manifest -i output_name=dv1KGPWGS -i config=DeepVariant \
  -i spark_default_parallelism=6400 --instance-count 80 -y --brief
```

- `vcf_manifest` is the manifest file from above
- `output_name` will be used in the output folder, and each individual output filename
- `spark_default_parallelism` controls the data partitioning
    - suggested setting: *N*÷10 (WES) or *N* (WGS), but not less than cluster vCPU count
- `--instance_count` controls the cluster size; each instance has 12 vCPUs, so 80 instances = 960 vCPUs total
    - suggested setting: *at least* 10 per TB of compressed input files

#### Options

**spVCF decoding.** Add `-i spvcf_decode=true` to output both spvcf.gz and decoded pVCF vcf.gz files (which will be much larger!).

**Filter regions.** To limit joint-calling to variants contained within given regions:

* BED file: `-i filter_bed=xgen_plus_spikein_100bpBuff.b38.bed`
* Contigs: `-i filter_contigs=chr1,chr2,chr3,chr4`

If both are supplied, then they're intersected: only variants in *both* a BED region and a filter contig will be called.

**Output file splitting.** By default, the app generates one spvcf.gz output file per contig. For larger cohorts, a BED file can be given to guide further splitting of the spVCF output files:

```bash
-i split_bed=GRCh38_60Mbp_shards.bed
```

The BED regions must cover the contigs to be processed without any gaps or overlaps.

**Joint calling options.**  The following additional argument can be added to

* call REF (0) instead of non-call (.) when there’s an overlapping ALT allele in a different pVCF record
* filter variants by quality score

```bash
-i java_options='-Dconfig.override.joint.gt.overlapMode=REF -Dconfig.override.discovery.minQUAL1=10 -Dconfig.override.discovery.minQUAL2=5'
```

To filter variants, set `minQUAL2 > 0` and `minQUAL1 >= minQUAL2`. This will include only variants with (i) one or more copy with quality at least `minQUAL1`, OR (ii) two or more copies with quality at least `minQUAL2`.

**gVCF left-alignment.** Add `-i ref_genome=GRC38.fa.gz` to run the [gvcf_norm](https://github.com/mlin/gvcf_norm) utility on each input file before further processing, with the given reference genome fa.gz.

#### Operational notes

When the job succeeds the spvcf.gz files will be output under the `/{OUTPUT_NAME}` folder in the current DNAnexus working directory.

Success or failure, the job also uploads log files under `/vcfGLuer_clusterlogs/job-xxxx/`. The cluster driver and workers each upload log tarballs under this folder every 5 minutes. We sometimes need to dig into these low-level logs to troubleshoot failures.

The most common failure mode is an `ExecutorLostFailure` exception reported in the DNAnexus job log (with a typically lengthy Java stack trace), most often due to an OOM error in one of the Spark instances. In this case it may help to increase the `spark_default_parallelism` and/or `instance_count`.
