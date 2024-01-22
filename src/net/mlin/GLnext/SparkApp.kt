@file:JvmName("SparkApp")

package net.mlin.GLnext
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.multiple
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.sksamuel.hoplite.*
import java.io.File
import java.util.Properties
import net.mlin.GLnext.data.*
import net.mlin.GLnext.joint.*
import net.mlin.GLnext.util.*
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.spark.api.java.JavaSparkContext
import org.jetbrains.kotlinx.spark.api.*

data class SparkConfig(val compressTempFiles: Boolean)
data class MainConfig(
    val complete: Boolean,
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
    val outputDir: String by
        argument(help = "Output directory for spVCF parts (mustn't already exist)")
    val manifest by
        option(help = "Input files are manifest(s) containing one VCF filename per line")
            .flag(default = false)
    val config: String by option(help = "Configuration preset name").default("DeepVariant.WGS")
    val filterBed: String? by
        option(help = "Call variants only within a region from this BED file")
    val filterContigs: String? by
        option(
            help = "Contigs to process, comma-separated (intersect with BED regions, if any)"
        )
    val splitBed: String? by
        option(help = "Guide spVCF part splitting using non-overlapping regions from this BED file")

    override fun run() {
        val logger = LogManager.getLogger("GLnext")
        logger.setLevel(Level.INFO)
        val cfg = loadConfig(logger, config)

        require(
            cfg.discovery.minQUAL1 >= cfg.discovery.minQUAL2,
            { "minQUAL1 should be at least minQUAL2" }
        )
        testDiploidSubroutines()

        var effInputFiles = inputFiles
        if (manifest) {
            effInputFiles = inputFiles.flatMap { fileReaderDetectGz(it).readLines() }.distinct()
        }

        val sparkConf = org.apache.spark.SparkConf()
        sparkConf.let {
            it.setAppName("GLnext")
            it.set("io.compression.codecs", compressionCodecsWithBGZF())
            // Disabling some Spark optimizations that seem to cause OOM problems for our usage
            // patterns.
            it.set("spark.sql.join.preferSortMergeJoin", "true")
            it.set("spark.sql.autoBroadcastJoinThreshold", "-1")
            it.set("spark.sql.execution.useObjectHashAggregateExec", "false")
            it.set("spark.sql.inMemoryColumnarStorage.compressed", "false")

            // set up Kryo; mostly we use Dataset<Row> but we do use JavaRDD for certain
            // specific purposes e.g. when we need low-level control of shuffle partitioning.
            it.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            it.set("spark.kryo.registrationRequired", "true")
            it.set("spark.kryoserializer.buffer.max", "2047m")
            // https://github.com/EsotericSoftware/kryo#reference-limits
            it.set("spark.kryo.referenceTracking", "false")
            it.registerKryoClasses(_classesForKryo)
        }

        if (cfg.spark.compressTempFiles) {
            sparkConf.let {
                it.set("spark.shuffle.compress", "true")
                it.set("spark.shuffle.spill.compress", "true")
                it.set("spark.broadcast.compress", "true")
                it.set("spark.checkpoint.compress", "true")
                it.set("spark.rdd.compress", "true")
                it.set("spark.io.compression.codec", "lz4")
                it.set("spark.io.compression.lz4.blockSize", "262144")
            }
        }

        withSpark(
            sparkConf = sparkConf,
            logLevel = SparkLogLevel.ERROR
        ) {
            val defaultParallelism = spark.sparkContext.defaultParallelism()

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
            logger.info("GLnext v${getProjectVersion()}")
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

            // load BEDs
            val jsc = JavaSparkContext(spark.sparkContext)
            val filterRangesB = filterBed?.let {
                val filterRanges = BedRanges(aggHeader.contigId, fileReaderDetectGz(it))
                jsc.broadcast(filterRanges)
            }
            logger.info("--filter-bed regions: ${filterRangesB?.value?.size?.pretty() ?: 0}")
            val filterRids = filterContigs?.let {
                it.split(",").map {
                    val ans = aggHeader.contigId.get(it)
                    require(ans != null, { "unknown contig $it in --filter-contigs" })
                    ans
                }.toSet()
            }
            logger.info("--filter-contigs regions: ${filterRids?.size?.pretty() ?: 0}")
            val splitRanges = readSplitBed(aggHeader.contigId, splitBed)
            logger.info("--split-bed regions: ${splitRanges.size.pretty()}")

            // accumulators
            val vcfRecordCount = spark.sparkContext.longAccumulator("input VCF records")
            val vcfRecordBytes = spark.sparkContext.longAccumulator("input VCF bytes)")
            val sparseEntryCount = spark.sparkContext.longAccumulator("sparse genotype entries")
            val revisedGenotypeCount = spark.sparkContext.longAccumulator("revised genotypes")

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
                cfg.discovery,
                aggHeader.contigId,
                vcfFilenamesDF,
                splitRanges,
                filterRids,
                filterRangesB,
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
                "GLnext_version=${getProjectVersion()}",
                "GLnext_config=$cfg"
            )
            val (spvcfHeader, pvcfLineCount, spvcfLines) = jointCall(
                cfg.joint,
                spark,
                aggHeader,
                variantsDbSparkFile,
                vcfFilenamesDF,
                pvcfHeaderMetaLines,
                sparseEntryCount,
                revisedGenotypeCount
            )
            check(pvcfLineCount == variantCount.toLong())
            logger.info("sparse genotype entries: ${sparseEntryCount.sum().pretty()}")
            logger.info("genotypes revised: ${revisedGenotypeCount.sum().pretty()}")
            logger.info("writing ${pvcfLineCount.pretty()} spVCF lines...")

            // write output spVCF files
            writeJointFiles(
                logger,
                aggHeader.contigs,
                splitRanges,
                spvcfHeader,
                spvcfLines,
                outputDir,
                variantCount
            )
        }
    }
}

fun getProjectVersion(): String {
    val props = Properties()
    props.load(
        Unit.javaClass.getClassLoader()
            .getResourceAsStream("META-INF/maven/net.mlin/GLnext/pom.properties")
    )
    return props.getProperty("version")
}

fun loadConfig(logger: Logger, name: String): MainConfig {
    var loader = ConfigLoader.Builder()
        .addFileExtensionMapping("toml", com.sksamuel.hoplite.toml.TomlParser())

    // derive inherited configuration filenames: for example, DeepVariant.WES.Extra inherits
    // DeepVariant.WES and DeepVariant
    val nameParts = name.split(".")
    val inheritedNames = (1..nameParts.size).map { nameParts.take(it).joinToString(".") }

    // add them in reverse order so that the most specific ones take precedence
    inheritedNames.reversed().forEach {
        val configFn = "/config/$it.toml"
        logger.info("load resource $configFn")
        loader = loader.addSource(PropertySource.resource(configFn))
    }
    loader = loader.addSource(PropertySource.resource("/config/main.toml"))

    val cfg = loader.build().loadConfigOrThrow<MainConfig>()
    require(cfg.complete, { "invalid config $name" })
    return cfg
}

val _classesForKryo = arrayOf(
    java.lang.Comparable::class.java,
    java.util.ArrayList::class.java,
    java.util.LinkedHashMap::class.java,
    java.util.PriorityQueue::class.java,
    IntPriorityQueue::class.java,
    kotlin.Pair::class.java,
    ByteArray::class.java,
    Array<ByteArray>::class.java,
    IntArray::class.java,
    Array<IntArray>::class.java,
    Class.forName("java.util.Collections\$SingletonList"),
    Class.forName("scala.reflect.ClassTag\$GenericClassTag"),
    org.apache.spark.sql.catalyst.InternalRow::class.java,
    Array<org.apache.spark.sql.catalyst.InternalRow>::class.java,
    org.apache.spark.sql.catalyst.expressions.GenericInternalRow::class.java,
    /*
     WARNING: non-Internal Row classes MUST NOT appear here; they're extremely inefficient to
     serialize, even with Kryo, because they ship the schema with every row. Instead of registering
     them here, we must make Spark use its SerDe framework on them instead.
     `git blame` this comment to find a commit including an example workaround.
     */
    org.apache.spark.sql.types.StructType::class.java,
    org.apache.spark.sql.types.StructField::class.java,
    Array<org.apache.spark.sql.types.StructField>::class.java,
    Class.forName("org.apache.spark.sql.types.ShortType$"),
    Class.forName("org.apache.spark.sql.types.IntegerType$"),
    Class.forName("org.apache.spark.sql.types.StringType$"),
    org.apache.spark.sql.types.Metadata::class.java,
    org.apache.spark.sql.execution.columnar.DefaultCachedBatch::class.java,
    org.apache.hadoop.fs.Path::class.java,
    _DigestedHeader::class.java,
    _FilenamesWithSameHeader::class.java,
    _CallsetDetailsPre::class.java,
    CallsetDetails::class.java,
    Array<CallsetDetails>::class.java,
    VcfHeaderLine::class.java,
    VcfHeaderLineKind::class.java,
    AggVcfHeader::class.java,
    GRange::class.java,
    BedRanges::class.java,
    net.mlin.iitj.IntegerIntervalTree::class.java,
    Array<net.mlin.iitj.IntegerIntervalTree>::class.java,
    Variant::class.java,
    JointConfig::class.java,
    JointGenotypeConfig::class.java,
    GenotypeRevisionConfig::class.java,
    JointFieldsGenerator::class.java,
    JointFormatField::class.java,
    GT_OverlapMode::class.java,
    CopiedFormatField::class.java,
    GQall_FormatField::class.java,
    DP_FormatField::class.java,
    AD_FormatField::class.java,
    PL_FormatField::class.java,
    PLall_FormatField::class.java,
    OL_FormatField::class.java,
    PartWritten::class.java,
    NthLargestInt::class.java,
    VariantStats::class.java,
    DiscoveredVariant::class.java
)
