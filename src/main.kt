@file:JvmName("vcfGLuer")
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.multiple
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.sksamuel.hoplite.*
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.*
import org.jetbrains.kotlinx.spark.api.*
import java.io.File
import java.net.URI
import java.util.Properties

data class SparkConfig(val compressTempRecords: Boolean, val compressTempFiles: Boolean)
data class VariantDiscoveryConfig(val minCopies: Int)
data class MainConfig(val spark: SparkConfig, val variantDiscovery: VariantDiscoveryConfig, val joint: JointConfig)

class CLI : CliktCommand() {
    val inputFiles: List<String> by argument(help = "Input VCF filenames (or manifest(s) with --manifest)").multiple(required = true)
    val pvcfDir: String by argument(help = "Output directory for pVCF parts (mustn't already exist)")
    val manifest by option(help = "Input files are manifest(s) containing one VCF filename per line").flag(default = false)
    val config: String by option(help = "Configuration preset name").default("DeepVariant")
    val binSize: Int by option(help = "Genome range bin size").int().default(16)
    val allowDuplicateSamples by option(help = "If a sample appears in multiple input callsets, use one arbitrarily instead of failing").flag(default = false)
    val deleteInputVcfs by option(help = "Delete input VCF files after loading them (DANGER!)").flag(default = false)

    // TODO: take a BED file of target regions

    override fun run() {
        val cfg = ConfigLoader.Builder()
            .addFileExtensionMapping("toml", com.sksamuel.hoplite.toml.TomlParser())
            .addSource(PropertySource.resource("/config/main.toml"))
            .addSource(PropertySource.resource("/config/$config.toml"))
            .build()
            .loadConfigOrThrow<MainConfig>()

        require(cfg.variantDiscovery.minCopies <= 1, { "unsupported variantDiscovery.minCopies" })

        var effInputFiles = inputFiles
        if (manifest) {
            effInputFiles = inputFiles.flatMap { java.io.File(it).readLines() }.distinct()
        }

        var sparkBuilder = org.apache.spark.sql.SparkSession.builder()
            .appName("vcfGLuer")
            // disabling broadcast hash join: during initial scale-testing spark 3.1.2 seemed to
            // use it on far-too-large datasets, leading to OOM failures
            .config("spark.sql.join.preferSortMergeJoin", true)
            .config("spark.sql.autoBroadcastJoinThreshold", -1)
            .config("io.compression.codecs", compressionCodecsWithBGZF())

        if (cfg.spark.compressTempFiles) {
            sparkBuilder = sparkBuilder
                .config("spark.shuffle.compress", true)
                .config("spark.broadcast.compress", true)
                .config("spark.checkpoint.compress", true)
                .config("spark.rdd.compress", true)
                .config("spark.io.compression.codec", "lz4")
                .config("spark.io.compression.lz4.blockSize", "262144")
        }

        withSpark(
            builder = sparkBuilder,
            logLevel = SparkLogLevel.ERROR
        ) {
            registerGRangeBinsUDF(spark)

            val logger = LogManager.getLogger("vcfGLuer")
            logger.setLevel(Level.INFO)

            logger.info("${System.getProperty("java.runtime.name")} ${System.getProperty("java.runtime.version")}")
            logger.info("Spark v${spark.version()}")
            logger.info("spark.default.parallelism: ${spark.sparkContext.defaultParallelism()}")
            logger.info("spark executors: ${spark.sparkContext.statusTracker().getExecutorInfos().size}")
            logger.info("Locale: ${java.util.Locale.getDefault()}")
            logger.info("vcfGLuer v${getProjectVersion()}")
            logger.info(cfg.toString())
            logger.info("binSize: $binSize")
            logger.info("input VCF files: ${effInputFiles.size.pretty()}")

            // load & aggregate all input VCF headers
            val aggHeader = aggregateVcfHeaders(spark, effInputFiles, allowDuplicateSamples = allowDuplicateSamples)

            logger.info("samples: ${aggHeader.samples.size.pretty()}")
            logger.info("callsets: ${aggHeader.callsetsDetails.size.pretty()}")
            listOf(
                ("FILTER" to VcfHeaderLineKind.FILTER),
                ("INFO" to VcfHeaderLineKind.INFO),
                ("FORMAT" to VcfHeaderLineKind.FORMAT)
            ).forEach {
                (kindName, kind) ->
                val fields = aggHeader.headerLines.filter { it.value.kind == kind }.map { it.value.id }.sorted().joinToString(" ")
                logger.info("$kindName: $fields")
            }
            logger.info("contigs: ${aggHeader.contigId.size.pretty()}")

            // load all VCF records
            val vcfRecordCount = spark.sparkContext.longAccumulator("original VCF records")
            val vcfRecordBytes = spark.sparkContext.longAccumulator("original VCF bytes)")
            readVcfRecordsDF(
                spark, aggHeader, compressEachRecord = cfg.spark.compressTempRecords, deleteInputVcfs = deleteInputVcfs,
                recordCount = vcfRecordCount, recordBytes = vcfRecordBytes
            ).withCached {
                val vcfRecordsDF = this

                // discover variants
                val variantsDF = discoverVariants(vcfRecordsDF, onlyCalled = cfg.variantDiscovery.minCopies > 0)

                // perform joint-calling
                val pvcfHeaderMetaLines = listOf(
                    "vcfGLuer_version=${getProjectVersion()}",
                    "vcfGLuer_config=$cfg"
                )
                val pvcfRecordCount = spark.sparkContext.longAccumulator("pVCF records")
                val pvcfRecordBytes = spark.sparkContext.longAccumulator("pVCF bytes")
                val (pvcfHeader, pvcfLines) = jointCall(cfg.joint, spark, aggHeader, variantsDF, vcfRecordsDF, binSize, pvcfHeaderMetaLines, pvcfRecordCount, pvcfRecordBytes)

                // write pVCF records (in parts)
                pvcfLines.write().option("compression", BGZFCodecClassName()).text(pvcfDir)

                logger.info("original VCF records: ${vcfRecordCount.sum().pretty()}")
                logger.info("original VCF bytes: ${vcfRecordBytes.sum().pretty()}")
                logger.info("pVCF variants: ${pvcfRecordCount.sum().pretty()}")
                logger.info("pVCF bytes: ${pvcfRecordBytes.sum().pretty()}")

                // write output VCF header & BGZF EOF marker
                writeHeaderAndEOF(pvcfHeader, pvcfDir)
            }
        }
    }
}

fun main(args: Array<String>) {
    CLI().main(args)
}

fun Int.pretty(): String = java.text.NumberFormat.getIntegerInstance().format(this)
fun Long.pretty(): String = java.text.NumberFormat.getIntegerInstance().format(this)

fun getProjectVersion(): String {
    val props = Properties()
    props.load(Unit.javaClass.getClassLoader().getResourceAsStream("META-INF/maven/net.mlin/vcfGLuer/pom.properties"))
    return props.getProperty("version")
}

fun writeHeaderAndEOF(headerText: String, dir: String) {
    val fs = getFileSystem(dir)

    fs.create(Path(dir, "00HEADER.bgz"), true).use {
        BGZFOutputStream(it).use {
            java.io.OutputStreamWriter(it, "UTF-8").use {
                it.write(headerText)
            }
        }
    }

    fs.create(Path(dir, ".wip.zzEOF.bgz"), true).use {
        it.write(htsjdk.samtools.util.BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK)
    }
    check(
        fs.rename(Path(dir, ".wip.zzEOF.bgz"), Path(dir, "zzEOF.bgz")),
        { "unable to finalize $dir/zzEOF.bgz" }
    )
}

fun getFileSystem(path: String): FileSystem {
    val normPath = if (path.startsWith("hdfs:") || path.startsWith("file://")) {
        path
    } else {
        "file://" + path
    }
    return FileSystem.get(URI(normPath), org.apache.spark.deploy.SparkHadoopUtil.get().conf())
}
