@file:JvmName("vcfGLuer")
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.arguments.argument
import com.github.ajalt.clikt.parameters.arguments.multiple
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.spark.sql.*
import org.jetbrains.kotlinx.spark.api.*

class CLI : CliktCommand() {
    val inputFiles: List<String> by argument(help = "Input VCF filenames (or manifest(s) with --manifest)").multiple(required = true)
    val pvcfDir: String by argument(help = "Output directory for pVCF parts (mustn't already exist)")
    val manifest by option(help = "Input files are manifest(s) containing one VCF filename per line").flag(default = false)
    val deleteInputVcfs by option(help = "Delete input VCF files after loading them (DANGER!)").flag(default = false)
    val compressTemp by option(help = "Compress Spark temporary files (marginal benefit on localhost)").flag(default = false)
    val binSize: Int by option(help = "Genome range bin size").int().default(100)

    override fun run() {

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

        if (compressTemp) {
            sparkBuilder = sparkBuilder
                .config("spark.shuffle.compress", true)
                .config("spark.broadcast.compress", true)
                .config("spark.checkpoint.compress", true)
                .config("spark.rdd.compress", true)
                .config("spark.io.compression.codec", "snappy")
                .config("spark.io.compression.snappy.blockSize", "262144")
        }

        withSpark(
            builder = sparkBuilder,
            logLevel = SparkLogLevel.ERROR
        ) {
            registerGRangeBinsUDF(spark)

            val logger = LogManager.getLogger("vcfGLuer")
            logger.setLevel(Level.INFO)
            logger.info("binSize: $binSize")
            logger.info("input VCF files: ${effInputFiles.size.pretty()}")

            // load & aggregate all input VCF headers
            val aggHeader = aggregateVcfHeaders(spark, effInputFiles)

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
            readVcfRecordsDF(spark, aggHeader, deleteInputVcfs = deleteInputVcfs, recordCount = vcfRecordCount).withCached {
                val vcfRecordsDF = this

                // discover variants
                val variantsDF = discoverVariants(vcfRecordsDF)

                // perform joint-calling
                val pvcfRecordCount = spark.sparkContext.longAccumulator("pVCF records")
                val pvcfLines = jointCall(spark, aggHeader, variantsDF, vcfRecordsDF, binSize, pvcfRecordCount)

                // write pVCF records (in parts)
                pvcfLines.write().option("compression", "snappy").text(pvcfDir)

                logger.info("original VCF records: ${vcfRecordCount.sum().pretty()}")
                logger.info("unique variants: ${pvcfRecordCount.sum().pretty()}")

                // atomically write output VCF header
                val headerFile = java.io.File(pvcfDir, ".wip._HEADER")
                headerFile.bufferedWriter().use { aggHeader.write(it) }
                check(headerFile.renameTo(java.io.File(pvcfDir, "_HEADER")), { "unable to finalize pVCF _HEADER" })
            }
        }
    }
}

fun main(args: Array<String>) {
    CLI().main(args)
}

fun Int.pretty(): String = java.text.NumberFormat.getIntegerInstance().format(this)
fun Long.pretty(): String = java.text.NumberFormat.getIntegerInstance().format(this)
