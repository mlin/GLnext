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
import org.apache.hadoop.fs.Path
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.sql.Dataset
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
        option(help = "only call variants within a region from this BED file")
    val deleteInputVcfs by
        option(help = "Delete input VCF files after loading them (DANGER!)").flag(default = false)

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
            val filterRangesB = filterBed?.let {
                val filterRanges = BedRanges(aggHeader.contigId, fileReaderDetectGz(it))
                logger.info("BED filter ranges: ${filterRanges.size}")
                JavaSparkContext(spark.sparkContext).broadcast(filterRanges)
            }

            // accumulators
            val vcfRecordCount = spark.sparkContext.longAccumulator("input VCF records")
            val vcfRecordBytes = spark.sparkContext.longAccumulator("input VCF bytes)")
            val pvcfRecordBytes = spark.sparkContext.longAccumulator("pVCF bytes")

            /*
              Discover all variants & collect them to a database file local to the driver.

              The list of variants, with associated stats, occupies an awkward middle ground --
              too large to have all executors keep it as a JVM heap data structure, and yet not
              large enough to warrant a partitioned DataFrame necessitating a gigantic shuffle of
              the input VCF records. Instead, we write them into a GenomicSQLite database file and
              (below) distribute this compressed file to all executors.
            */
            val vcfFilenamesDF = aggHeader.vcfFilenamesDF(spark)
            val (variantCount, variantsDbFilename) = collectAllVariantsDb(
                aggHeader.contigId,
                vcfFilenamesDF,
                filterRangesB,
                onlyCalled = cfg.discovery.minCopies > 0,
                vcfRecordCount,
                vcfRecordBytes
            )

            // report accumulators
            logger.info("input VCF records: ${vcfRecordCount.sum().pretty()}")
            logger.info("input VCF bytes: ${vcfRecordBytes.sum().pretty()}")
            logger.info("joint variants: ${variantCount.pretty()}")
            val variantsDbFileSize = File(variantsDbFilename).length()
            logger.info("variants DB compressed: ${variantsDbFileSize.pretty()} bytes")

            // broadcast the variants database to the same temp filename on each executor
            val variantsDbCopies = 1 + broadcastLargeFile(vcfFilenamesDF, variantsDbFilename)
            logger.info("variants DB broadcast: ${variantsDbCopies.pretty()} copies")

            // perform joint-calling
            val pvcfHeaderMetaLines = listOf(
                "vcfGLuer_version=${getProjectVersion()}",
                "vcfGLuer_config=$cfg"
            )
            val (pvcfHeader, pvcfLines) = jointCall(
                logger, cfg.joint, spark, aggHeader,
                variantCount, variantsDbFilename, vcfFilenamesDF,
                pvcfHeaderMetaLines, pvcfRecordBytes
            )

            // write pVCF lines (in BGZF parts)
            pvcfLines.saveAsTextFile(pvcfDir, BGZFCodec::class.java)
            logger.info("pVCF bytes: ${pvcfRecordBytes.sum().pretty()}")

            // reorganize part files
            reorgJointFiles(spark, pvcfDir, aggHeader)

            // write output VCF header & BGZF EOF marker
            writeHeaderAndEOF(pvcfHeader, pvcfDir)
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

/**
 * Given a filename local to the driver, broadcast it to all executors (with at least one partition
 * of someDataset) saved to the same local filename, which must not yet exist on any of them.
 *
 * Leveraging Spark's TorrentBroadcast in this way should be less bottlenecked than going through
 * HDFS. However, large files require multiple rounds since broadcast is constrained by ByteArray
 * max size.
 */
fun <T> broadcastLargeFile(
    someDataset: Dataset<T>,
    filename: String,
    chunkSize: Int = 1073741824
): Int {
    val sc = someDataset.sparkSession().sparkContext
    val jsc = JavaSparkContext(sc)
    var fileSize = 0L

    val chunkFilenames = readFileChunks(filename, chunkSize).mapIndexed { chunkNum, chunk ->
        val chunkFilename = filename + String.format(".chunk-%04d", chunkNum)
        val chunkB = jsc.broadcast(chunk)
        someDataset.foreachPartition(
            ForeachPartitionFunction<T> {
                val localChunk = File(chunkFilename)
                if (localChunk.createNewFile()) {
                    localChunk.writeBytes(chunkB.value)
                }
            }
        )
        chunkB.unpersist()
        fileSize += chunk.size.toLong()
        chunkFilename
    }.toList()

    val copies = sc.longAccumulator("broadcast copies of " + filename)
    someDataset.foreachPartition(
        ForeachPartitionFunction<T> {
            val localCopy = File(filename)
            if (localCopy.createNewFile()) {
                concatFiles(chunkFilenames, filename, chunkSize)
                check(localCopy.length() == fileSize)
                chunkFilenames.forEach { File(it).delete() }
                copies.add(1L)
            }
        }
    )

    return copies.sum().toInt()
}