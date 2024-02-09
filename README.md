# GLnext

**NOTICE: this project is public for our collaborators; it's not yet ready for general use!**

GLnext is a scalable tool for gVCF merging and joint variant calling in population-scale sequencing. It's a successor to [GLnexus](https://github.com/dnanexus-rnd/GLnexus), but shares no code and:

* runs on Apache Spark at scale
* simplifies the project VCF (pVCF) to represent only one ALT allele per line
* generates [spVCF](https://github.com/mlin/spVCF) natively (decodes to standard pVCF)

## Building

**First check our [Releases](https://github.com/mlin/GLnext/releases) for a prebuilt JAR file!**

Requirements: x86-64 platform, Linux or macOS, JDK 11+, Apache Maven.

```
git clone --recursive https://github.com/mlin/GLnext.git
cd GLnext
mvn package
```

and find the JAR file under `target/`.

To run some basic tests,

```
export SPARK_HOME=/path/to/spark-3.3.4-bin-hadoop3
prove -v test/dv1KGP.t
```

## Running GLnext

General requirements: x86-64 platform, Linux or macOS, Java 11+, Spark 3.3.x

Compatibility with other Spark versions is not assured. Also, the JAR uses native libraries for x86-64 only.

### Local

```
export SPARK_HOME=/path/to/spark-3.3.4-bin-hadoop3

_JAVA_OPTIONS="
    -Dspark.default.parallelism=$(nproc)
    -Dspark.sql.shuffle.partitions=$(nproc)
" $SPARK_HOME/bin/spark-submit --master 'local[*]' \
    GLnext-XXXX.jar --config DeepVariant.WGS \
    /path/to/sample1.gvcf.gz /path/to/sample2.gvcf.gz ... \
    /path/to/outputs/myCohort
```

The output spvcf.gz files, one per chromosome, are saved to the output directory set in the last argument. Its last path component (myCohort) is used in the individual output filenames.

To decode the spvcf.gz files to standard vcf.gz, download the [spvcf](https://github.com/mlin/spVCF) utility and run each file through `bgzip -dc myCohort_XXXX.spvcf.gz | spvcf decode | bgzip -@4 > myCohort_XXXX.vcf.gz`. Or all at once:

```
cd /path/to/outputs/myCohort
wget https://github.com/mlin/spVCF/releases/download/v1.3.2/spvcf
chmod +x spvcf
ls -1 *.spvcf.gz | parallel -t '
    bgzip -dc {} | ./spvcf decode -q | bgzip > $(basename {} .spvcf.gz).vcf.gz
'
```

However, spVCF decoding is usually fast enough to run on-the-fly, piping into downstream analysis tools, instead of storing the much larger vcf.gz files.

If you have a lot of input samples, then prepare a manifest file with one gVCF path per line, and pass GLnext `--manifest manifestFile.txt` instead of the individual paths. And, you'll probably hit out-of-memory errors until you edit the `_JAVA_OPTIONS` to increase the partitioning or (as always with Spark) tune [many other settings](https://spark.apache.org/docs/3.3.4/configuration.html#memory-management).

### Google Cloud Dataproc

First, upload to Google Cloud Storage:

1. GLnext JAR file
1. gvcf.gz input files
1. gVCF manifest file with one gs:// URI per line

Then:

```
gcloud dataproc batches submit spark \
    --region=us-west1 --version=1.1 \
    --jars=gs://MYBUCKET/GLnext-XXXX.jar \
    --class=net.mlin.GLnext.SparkApp \
    --properties=spark.default.parallelism=256,spark.sql.shuffle.partitions=256,spark.reducer.fetchMigratedShuffle.enabled=true \
    -- \
    --config DeepVariant.WGS \
    --manifest gs://MYBUCKET/in/gvcf_manifest.txt \
    gs://MYBUCKET/out/myCohort
```

The spvcf.gz files are saved to the storage folder set in the last argument. You may then decide how and when to `spvcf decode` them to standard VCF; perhaps piping into downstream analysis tools, or using your preferred batch workflow runner.

### DNAnexus

Build the [DNAnexus Spark App](https://documentation.dnanexus.com/developer/apps/developing-spark-apps):

```
dx build dx/GLnext
```

And see [dx/GLnext/README.md](dx/GLnext/README.md) for detailed usage instructions.

## Default pVCF representation

In the GLnext \[s\]pVCF, all "sites" (lines) represent only one ALT allele, written in [normalized](https://genome.sph.umich.edu/wiki/Variant_Normalization) form. Distinct overlapping ALT alleles are presented on nearby lines. In a genotype entry, if the sample has one or more copies of an overlapping ALT allele *other than* the one presented on the current line, then the `GT` is either half-called or non-called (`./0` `./1` or `./.`) and the `OL` field is set to the overlapping ALT copy number.

Experience has shown that this approach is closer to the typical practice of statistical analyses on large cohorts, compared to [multiallelic sites](https://github.com/dnanexus-rnd/GLnexus/wiki/Reading-GLnexus-pVCFs). It's less optimized for family-level analyses focused on compound heterozygote genotypes (1/2 etc.).

By default, GLnext keeps only `GT` and `DP` in pVCF entries deriving from gVCF reference bands. Other QC fields like `GQ`, `PL`, etc. are not very meaningful *when derived from reference bands*, and omitting them reduces the file size considerably. The tool can be reconfigured to propagate them if needed (see below). Beyond that choice, GLnext generates spVCF losslessly, *without* the rounding of `DP` values in spVCF's "squeeze" feature. If that's palatable, then the GLnext spVCF can be re-encoded with `spvcf decode | spvcf encode --squeeze` to reduce its size further.

## Options

**Configurations.** Available settings of `--config`:

* `DeepVariant.WGS`
* `DeepVariant.AllQC.WGS`
* `DeepVariant.WES`
* `DeepVariant.AllQC.WES`

The WGS and WES settings provide different calibrations for the joint genotype revision calculations (identical to GLnexus).

The AllQC configurations keep all QC values from reference bands, as discussed above. This should be paired with the `spvcf decode --with-missing-fields` argument to make all FORMAT fields explicit.

**Allele quality filtering.** Unlike GLnexus, GLnext does not apply any variant quality filters by default: any ALT allele with at least one copy called is included in the output. Compared to traditional multiallelic pVCF, the impact of many lower-quality variants is mitigated by the combination of our biallelic representation and spVCF encoding. 

Nonetheless, quality filters may be practically desirable at a certain scale, and can be enabled by setting Java options/properties:

```
-Dconfig.override.discovery.minQUAL1=10 -Dconfig.override.discovery.minQUAL2=5
```

These thresholds include alleles with at least one copy called with Phred QUAL≥10, *or* at least two copies with QUAL≥5. (Analogous to GLnexus min_AQ1 and min_AQ2.)

**Region filter.** To limit the output spVCF to variants contained within given regions:

* BED file: `--filter-bed myExomeKit.bed`
* Contigs: `--filter-contigs chr1,chr2,chr3,chr4`

If both are supplied, then they're intersected: only variants in *both* a BED region and a filter contig will be called.

**Output file splitting.** By default, the app generates one spvcf.gz output file per contig. For larger cohorts where per-contig files are themselves unwieldy, a BED file can be given to guide further splitting of the spVCF output files with: `--split-bed GRCh38_60Mbp_shards.bed`. The BED regions must fully cover the contigs to be processed without any gaps or overlaps.

