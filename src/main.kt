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
import org.jetbrains.kotlinx.spark.api.*

class CLI : CliktCommand() {
    val inputFiles: List<String> by argument(help = "Input VCF filenames (or manifest(s) with --manifest)").multiple(required = true)
    val pvcfDir: String by argument(help = "Output directory for pVCF parts (mustn't already exist)")
    val manifest by option(help = "Input files are manifest(s) containing one VCF filename per line").flag(default = false)
    val deleteInputVcfs by option(help = "Delete input VCF files after loading them (DANGER!)").flag(default = false)
    val binSize: Int by option(help = "Genome range bin size").int().default(100)

    override fun run() {

        var effInputFiles = inputFiles
        if (manifest) {
            effInputFiles = inputFiles.flatMap { java.io.File(it).readLines() }.distinct()
        }

        withSpark(
            props = mapOf(
                "spark.broadcast.compress" to true,
                "spark.checkpoint.compress" to true,
                "spark.rdd.compress" to true,
                "spark.io.compression.codec" to "snappy",
                "spark.io.compression.snappy.blockSize" to "262144"
            )
        ) {
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
            val binnedRecordCount = spark.sparkContext.longAccumulator("binned VCF records")

            readVcfRecords(spark, aggHeader, deleteInputVcfs = deleteInputVcfs, recordCount = vcfRecordCount).withCached {
                val vcfRecords = this

                // TODO: aggregate & bin variants in a smarter way; this suffices when we don't need to
                // reduce any variant stats

                // bin & join records and variants
                val binnedRecords = vcfRecords.binned(binSize, binnedRecordCount)
                val binnedVariants = binnedRecords.getBinnedVariants()
                val jointData = joinVariantsAndVcfRecords(binnedVariants, binnedRecords)

                // generate & sort pVCF lines
                val pvcfRecordCount = spark.sparkContext.longAccumulator("pVCF records")
                val pvcf = jointData.jointCall(spark, aggHeader, pvcfRecordCount)
                    .orderBy("first.range.rid", "first.range.beg", "first.range.end", "first.alt")
                val pvcfLines = pvcf.select(pvcf.col("second").`as`<String>())

                // write pVCF lines
                // snappy is preferred because we can easily read it from the command line using
                //   snzip -dc -t hadoop-snappy ...
                pvcfLines.write().option("compression", "snappy").text(pvcfDir)

                logger.info("variants: ${pvcfRecordCount.sum().pretty()}")
                logger.info("original VCF records: ${vcfRecordCount.sum().pretty()}")
                // misleading because binnedRecords is computed twice:
                // logger.info("binned VCF records: ${binnedRecordCount.sum().pretty()}")

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
