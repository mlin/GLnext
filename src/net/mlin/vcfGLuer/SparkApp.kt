@file:JvmName("SparkApp")

package net.mlin.vcfGLuer
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.multiple
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.sksamuel.hoplite.*
import java.io.File
import java.util.Properties
import net.mlin.vcfGLuer.data.*
import net.mlin.vcfGLuer.joint.*
import net.mlin.vcfGLuer.util.*
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.api.java.JavaSparkContext
import org.jetbrains.kotlinx.spark.api.*

data class SparkConfig(val compressTempFiles: Boolean)
data class DiscoveryConfig(val allowDuplicateSamples: Boolean, val minCopies: Int)
data class MainConfig(
    val spark: SparkConfig,
    val discovery: DiscoveryConfig,
    val joint: JointConfig
)

fun main(args: Array<String>) {
    CLI().main(args)
}

class CLI : CliktCommand() {
    val inputFiles: List<String> by
        argument(help = "Input VCF filenames (or manifest(s) with --manifest)")
            .multiple(required = true)
    val pvcfDir: String by
        argument(help = "Output directory for pVCF parts (mustn't already exist)")
    val manifest by
        option(help = "Input files are manifest(s) containing one VCF filename per line")
            .flag(default = false)
    val config: String by option(help = "Configuration preset name").default("DeepVariant")
    val filterBed: String? by
        option(help = "Call variants only within a region from this BED file")
    val filterContigs: String? by
        option(
            help = "Contigs to process, comma-separated (intersect with BED regions, if any)"
        )
    val splitBed: String? by
        option(help = "Guide pVCF part splitting using non-overlapping regions from this BED file")
    val tmpDir: String? by
        option(
            help = "Shared (HDFS) temporary directory"
        )

    override fun run() {
        val cfg = ConfigLoader.Builder()
            .addFileExtensionMapping("toml", com.sksamuel.hoplite.toml.TomlParser())
            .addSource(PropertySource.resource("/config/$config.toml"))
            .addSource(PropertySource.resource("/config/main.toml"))
            .build()
            .loadConfigOrThrow<MainConfig>()

        require(cfg.discovery.minCopies <= 1, { "unsupported discovery.minCopies" })

        var effInputFiles = inputFiles
        if (manifest) {
            effInputFiles = inputFiles.flatMap { java.io.File(it).readLines() }.distinct()
        }

        var sparkBuilder = org.apache.spark.sql.SparkSession.builder()
            .appName("vcfGLuer")
            .config("io.compression.codecs", compressionCodecsWithBGZF())
            // Disabling some Spark optimizations that seem to cause OOM problems for our usage
            // patterns.
            .config("spark.sql.join.preferSortMergeJoin", true)
            .config("spark.sql.autoBroadcastJoinThreshold", -1)
            .config("spark.sql.execution.useObjectHashAggregateExec", false)
            .config("spark.sql.inMemoryColumnarStorage.compressed", false)

        if (cfg.spark.compressTempFiles) {
            sparkBuilder = sparkBuilder
                .config("spark.shuffle.compress", true)
                .config("spark.shuffle.spill.compress", true)
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
            val defaultParallelism = spark.sparkContext.defaultParallelism()
            val logger = LogManager.getLogger("vcfGLuer")
            logger.setLevel(Level.INFO)

            logger.info(
                "${System.getProperty("java.runtime.name")}" +
                    " ${System.getProperty("java.runtime.version")}"
            )
            logger.info("Spark v${spark.version()}")
            logger.info("spark.default.parallelism: $defaultParallelism")
            logger.info(
                "spark executors: " +
                    spark.sparkContext.statusTracker().getExecutorInfos().size.toString()
            )
            logger.info("Locale: ${java.util.Locale.getDefault()}")
            java.sql.DriverManager.getConnection("jdbc:genomicsqlite::memory:").use {
                val sqliteVersion = (it as org.sqlite.SQLiteConnection).libversion()
                logger.info("SQLite v$sqliteVersion")
                val genomicsqliteVersion = net.mlin.genomicsqlite.GenomicSQLite.version(it)
                logger.info("GenomicSQLite $genomicsqliteVersion")
            }
            logger.info("vcfGLuer v${getProjectVersion()}")
            logger.info(cfg.toString())
            logger.info("input VCF files: ${effInputFiles.size.pretty()}")

            // load & aggregate all input VCF headers
            val aggHeader = aggregateVcfHeaders(
                spark,
                effInputFiles,
                allowDuplicateSamples = cfg.discovery.allowDuplicateSamples
            )

            logger.info("samples: ${aggHeader.samples.size.pretty()}")
            logger.info("callsets: ${aggHeader.callsetsDetails.size.pretty()}")
            listOf(
                ("FILTER" to VcfHeaderLineKind.FILTER),
                ("INFO" to VcfHeaderLineKind.INFO),
                ("FORMAT" to VcfHeaderLineKind.FORMAT)
            ).forEach {
                    (kindName, kind) ->
                val fields = aggHeader.headerLines
                    .filter { it.value.kind == kind }
                    .map { it.value.id }.sorted()
                    .joinToString(" ")
                logger.info("$kindName: $fields")
            }
            logger.info("contigs: ${aggHeader.contigId.size.pretty()}")

            // load filter ranges BED, if any
            val jsc = JavaSparkContext(spark.sparkContext)
            val filterRangesB = filterBed?.let {
                val filterRanges = BedRanges(aggHeader.contigId, fileReaderDetectGz(it))
                logger.info("BED filter ranges: ${filterRanges.size}")
                jsc.broadcast(filterRanges)
            }
            val filterRids = filterContigs?.let {
                it.split(",").map {
                    val ans = aggHeader.contigId.get(it)
                    require(ans != null, { "unknown contig $it in --filter-contigs" })
                    ans
                }.toSet()
            }

            // accumulators
            val vcfRecordCount = spark.sparkContext.longAccumulator("input VCF records")
            val vcfRecordBytes = spark.sparkContext.longAccumulator("input VCF bytes)")

            /*
              Discover all variants & collect them to a database file local to the driver.

              The list of variants, with associated stats, occupies an awkward middle ground --
              too large to have all executors keep it as a JVM heap data structure, and yet not
              large enough to warrant a partitioned DataFrame necessitating a gigantic shuffle of
              the input VCF records. Instead, we write them into a GenomicSQLite database file and
              (below) distribute this compressed file to all executors.
            */
            val vcfFilenamesDF = aggHeader.vcfFilenamesDF(spark)
            val (variantCount, variantsDbLocalFilename) = collectAllVariantsDb(
                aggHeader.contigId,
                vcfFilenamesDF,
                filterRids,
                filterRangesB,
                onlyCalled = cfg.discovery.minCopies > 0,
                vcfRecordCount,
                vcfRecordBytes
            )
            // broadcast the variants DB file to all executors
            jsc.addFile(variantsDbLocalFilename)
            val variantsDbSparkFile = File(variantsDbLocalFilename).name

            // report accumulators
            logger.info("input VCF records: ${vcfRecordCount.sum().pretty()}")
            logger.info("input VCF bytes: ${vcfRecordBytes.sum().pretty()}")
            logger.info("joint variants: ${variantCount.pretty()}")
            val variantsDbFileSize = File(variantsDbLocalFilename).length()
            logger.info("variants DB compressed: ${variantsDbFileSize.pretty()} bytes")

            // perform joint-calling
            logger.info("genotyping...")
            val pvcfHeaderMetaLines = listOf(
                "vcfGLuer_version=${getProjectVersion()}",
                "vcfGLuer_config=$cfg"
            )
            val (pvcfHeader, pvcfLineCount, pvcfLines) = jointCall(
                cfg.joint,
                spark,
                aggHeader,
                variantsDbSparkFile,
                vcfFilenamesDF,
                pvcfHeaderMetaLines
            )
            check(pvcfLineCount == variantCount.toLong())
            logger.info("writing ${pvcfLineCount.pretty()} pVCF lines...")

            // write output pVCF files
            writeJointFiles(
                logger,
                aggHeader.contigs,
                readSplitBed(aggHeader.contigId, splitBed),
                pvcfHeader,
                pvcfLines,
                pvcfDir,
                variantCount
            )
        }
    }
}

fun getProjectVersion(): String {
    val props = Properties()
    props.load(
        Unit.javaClass.getClassLoader()
            .getResourceAsStream("META-INF/maven/net.mlin/vcfGLuer/pom.properties")
    )
    return props.getProperty("version")
}
