import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.*
import org.apache.spark.sql.SparkSession
import org.jetbrains.kotlinx.spark.api.*
import java.io.OutputStreamWriter
import kotlin.math.log10

// Reorganize the pVCF parts written out by spark:
// Read the first VcfRecord from each part to infer (i) which parts may span multiple CHROMs and
// (ii) the genomic ranges covered by the remaining parts.
// For parts (ii): rename them so that filename indicates the genomic range
// For parts (i): split them by CHROM, naming each subpart according to the same schema

fun reorgJointFiles(spark: SparkSession, pvcfDir: String, aggHeader: AggVcfHeader) {
    val logger = LogManager.getLogger("vcfGLuer")
    val jsc = JavaSparkContext(spark.sparkContext)
    // limits parallelism of tiny HDFS requests to prevent overloading metadata server
    val parallelism = 16

    // List part filenames in pvcfDir
    val partBasenames = getFileSystem(pvcfDir).listSequence(pvcfDir, false).toList()
        .filter { it.isFile() && it.getPath().getName().startsWith("part-") }
        .map { it.getPath().getName() }
        .sorted()
    // TODO sanity-check basenames are the complete sequence...

    // Read the first VcfRecord GRange from each part
    val aggHeaderB = jsc.broadcast(aggHeader)
    val partsAndFirstRange = jsc.parallelize(partBasenames, parallelism).mapPartitions(
        object : FlatMapFunction<Iterator<String>, Pair<String, GRange>> {
            override fun call(basenames: Iterator<String>): Iterator<Pair<String, GRange>> {
                val fs = getFileSystem(pvcfDir)
                return basenames.asSequence().map {
                    basename ->
                    vcfInputStream("$pvcfDir/$basename", fs).bufferedReader().useLines {
                        val rec = parseVcfRecord(aggHeaderB.value.contigId, -1, it.first())
                        basename to rec.range
                    }
                }.iterator()
            }
        }
    ).collect().sortedBy { it.first }.toTypedArray()

    check(
        partsAndFirstRange.toList().sortedBy { it.second } == partsAndFirstRange.toList(),
        { "inconsistent part # and GRange order" }
    )

    // Infer which parts have content from only one chromosome, storing the rid and first POS.
    // -1 for parts potentially spanning multiple chromosomes.
    val classifiedParts: List<Triple<Short, Int, String>> = partsAndFirstRange.toList().mapIndexed {
        i, (basename, firstRange) ->
        val oneChrom = i + 1 < partsAndFirstRange.size && partsAndFirstRange[i + 1].second.rid == firstRange.rid
        if (oneChrom) Triple(firstRange.rid, firstRange.beg, basename) else Triple(-1, -1, basename)
    }
    logger.info("Initial output part count = ${classifiedParts.size}")

    // Split parts potentially spanning multiple chromosomes
    val partsToSplit = classifiedParts.filter { it.first < 0 }.map { it.third }
    var splitParts: List<Triple<Short, Int, String>> = emptyList()
    if (!partsToSplit.isEmpty()) {
        logger.info("Splitting output part files potentially spanning multiple chromosomes:")
        partsToSplit.forEach {
            logger.info("  $it")
        }
        splitParts = jsc.parallelize(partsToSplit).flatMap(
            object : FlatMapFunction<String, Triple<Short, Int, String>> {
                override fun call(partBasename: String): Iterator<Triple<Short, Int, String>> {
                    return splitByChr(pvcfDir, partBasename, aggHeaderB.value).iterator()
                }
            }
        ).collect()
    }

    // Consolidate the part list and calculate a last pos for each (-1 for chrom end)
    val revisedParts = (classifiedParts.filter { it.first >= 0 } + splitParts)
        .sortedWith(compareBy({ it.first }, { it.second })).toTypedArray()
    val rangeParts = revisedParts.mapIndexed {
        i, (rid, firstPos, basename) ->
        val lastPos = if (i + 1 < revisedParts.size && revisedParts[i + 1].first == rid) revisedParts[i + 1].second else -1
        i to Triple(rid, (firstPos to lastPos), basename)
    }
    val finalPartCount = rangeParts.size
    val finalPartCountDigits = 1 + log10(finalPartCount.toDouble()).toInt()

    // Rename all the parts
    val newNames = jsc.parallelize(rangeParts, parallelism).mapPartitions(
        object : FlatMapFunction<Iterator<Pair<Int, Triple<Short, Pair<Int, Int>, String>>>, String> {
            override fun call(parts: Iterator<Pair<Int, Triple<Short, Pair<Int, Int>, String>>>): Iterator<String> {
                val fs = getFileSystem(pvcfDir)
                return parts.asSequence().map {
                    (i, t) ->
                    val (rid, pos, basename) = t
                    val (posBeg, posEnd) = pos
                    val chrom = aggHeaderB.value.contigs[rid.toInt()]
                    val iPad = (i + 1).toString().padStart(finalPartCountDigits, '0')
                    val cPad = finalPartCount.toString().padStart(finalPartCountDigits, '0')
                    val posEndStr = if (posEnd >= 0) posEnd.toString() else "end"
                    val basename2 = "part-${iPad}of$cPad-$chrom-$posBeg-$posEndStr.bgz"
                    fs.rename(Path(pvcfDir, basename), Path(pvcfDir, basename2))
                    basename2
                }.iterator()
            }
        }
    ).collect()
    check(newNames.size == finalPartCount)
    logger.info("Final output part count = $finalPartCount")
    check(newNames.distinct().sorted() == newNames.sorted())
}

fun splitByChr(pvcfDir: String, partBasename: String, aggHeader: AggVcfHeader): List<Triple<Short, Int, String>> {
    val fs = getFileSystem(pvcfDir)

    val ans: MutableList<Triple<Short, Int, String>> = mutableListOf()
    var rid: Short = -1

    vcfInputStream("$pvcfDir/$partBasename", fs).bufferedReader().useLines {
        var writer: OutputStreamWriter? = null
        for (line in it) {
            val rec = parseVcfRecord(aggHeader.contigId, -1, line)
            if (rec.range.rid != rid) {
                if (writer != null) {
                    writer.close()
                }
                rid = rec.range.rid

                val tempName = "${aggHeader.contigs[rid.toInt()]}_${rec.range.beg.toString().padStart(9,'0')}.bgz.wip"
                writer = OutputStreamWriter(BGZFOutputStream(fs.create(Path(pvcfDir, tempName))), "UTF-8")
                ans.add(Triple(rid, rec.range.beg, tempName))
            }
            check(writer != null)
            writer.write(line)
            writer.write("\n")
        }

        if (writer != null) {
            writer.close()
        }
    }

    check(fs.delete(Path(pvcfDir, partBasename), false), { "unable to delete $partBasename" })

    return ans
}

fun FileSystem.listSequence(path: String, recursive: Boolean): Sequence<org.apache.hadoop.fs.LocatedFileStatus> {
    val it = getFileSystem(path).listFiles(Path(path), recursive)
    return sequence {
        while (it.hasNext()) {
            yield(it.next())
        }
    }
}
