/**
 * Tools for reading VCF file headers and quickly aggregating many of them (e.g. 1M+ GVCFs)
 *
 * Also contemplates that one logical "callset" might materialize as several VCF files (e.g. one
 * file per chromosome). The assumption is that files with identical headers (including the sample
 * identifier(s) on the #CHROM line) comprise one callset.
 */
package net.mlin.GLnext.data
import net.mlin.GLnext.util.*
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.function.Function
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

/**
 * VCF header contig/FILTER/INFO/FORMAT lines
 */
enum class VcfHeaderLineKind { FILTER, INFO, FORMAT, CONTIG }
data class VcfHeaderLine(
    val kind: VcfHeaderLineKind,
    val id: String,
    val lineText: String,
    val ord: Int = -1
) :
    Comparable<VcfHeaderLine> {
    override fun compareTo(other: VcfHeaderLine) = compareValuesBy(
        this,
        other,
        { it.kind },
        { it.ord },
        { it.id },
        { it.lineText }
    )
}
typealias VcfHeaderLineIndex = Map<Pair<VcfHeaderLineKind, String>, VcfHeaderLine>

/**
 * Details specific to one callset: for each sample in the original callset, its index in the
 * aggregated sample list
 */
data class CallsetDetails(val callsetFilenames: List<String>, val callsetSamples: IntArray)

/**
 * Meta header aggregated from all input VCF files (header lines deduplicated & checked for
 * consistency, with a global sample order and the mapping of each file's sample order thereunto).
 * This is all information about the input VCFs, preceding generation of the output pVCF's header
 * (JointHeader).
 */
data class AggVcfHeader(
    // Distinct header lines (keyed by kind & ID)
    val headerLines: VcfHeaderLineIndex,
    // Contig names & index thereof
    val contigs: Array<String>,
    val contigId: Map<String, Short>,
    // Aggregated sample list (distinct & ordered)
    val samples: Array<String>,
    // Many-to-one mapping of VCF filenames to callset ID (index in callsetsDetails)
    val filenameCallsetId: Map<String, Int>,
    // Info about each logical callset
    val callsetsDetails: Array<CallsetDetails>
)

// intermediates used within aggregateVcfHeaders (Kryo-serializable)
data class _DigestedHeader(val filename: String, val headerDigest: String, val header: String)
data class _FilenamesWithSameHeader(val header: String, val filenames: List<String>)
data class _CallsetDetailsPre(
    val callsetId: Int,
    val filenames: List<String>,
    val samples: List<String>
)

/**
 * Use Spark to load all VCF headers and aggregate them
 */
fun aggregateVcfHeaders(
    spark: org.apache.spark.sql.SparkSession,
    filenames: List<String>,
    allowDuplicateSamples: Boolean = false
): AggVcfHeader {
    val jsc = JavaSparkContext(spark.sparkContext())
    val exampleFilename = filenames.first()
    // pattern for recognizing artifically-multiplied files (for scalability testing)
    val multiplyPattern = System.getenv("_multiply")?.let {
        if (!it.isEmpty()) Regex("^_x(\\d+)_.+") else null
    }

    // read the headers of all VCFs, and group the filenames by common header
    val filenamesByHeader = jsc.parallelizeEvenly(filenames)
        .mapPartitions(
            FlatMapFunction<Iterator<String>, _DigestedHeader> {
                    filenamesPart ->
                val fs = getFileSystem(exampleFilename)
                filenamesPart.asSequence().map {
                    val (headerDigest, header) = readVcfHeader(it, fs)
                    val headerDigest2 = headerDigest + (
                        // give multiplied headers artificially distinct digests
                        multiplyPattern?.find(Path(it).getName())
                            ?.let { "_x${it.groupValues.get(1)}" } ?: ""
                        )
                    _DigestedHeader(it, headerDigest2, header)
                }.iterator()
            }
        )
        .groupBy(Function<_DigestedHeader, String> { it.headerDigest })
        .mapValues(
            Function<Iterable<_DigestedHeader>, _FilenamesWithSameHeader> {
                    dhs ->
                var header: String = ""
                val callsetFilenames = dhs.map {
                    header = it.header
                    it.filename
                }.asSequence().toList().sorted()
                _FilenamesWithSameHeader(header, callsetFilenames)
            }
        ).cache()

    try {
        // assign callsetIds as indexes into an array of header digests (arbitrary order)
        val digestToCallsetId = jsc.broadcast(
            filenamesByHeader.keys().collect().sorted().mapIndexed {
                    callsetId, digest ->
                digest to callsetId
            }.toMap()
        )

        // collect CallsetDetails (precursor)
        val callsetsDetailsPre = filenamesByHeader.map(
            Function<scala.Tuple2<String, _FilenamesWithSameHeader>, _CallsetDetailsPre> {
                val callsetId = digestToCallsetId.value.get(it._1)!!
                // read samples from last line of header
                val lastHeaderLine = it._2.header.trimEnd('\n').splitToSequence("\n").last()
                require(lastHeaderLine.startsWith("#CHROM"))
                val samples = lastHeaderLine.splitToSequence("\t").drop(9).toList()
                val exemplarFilename = Path(it._2.filenames.first()).getName()
                require(samples.toSet().size == samples.size, {
                    "$exemplarFilename header has duplicate sample names"
                })
                // put prefix on samples from artifically-multiplied files
                val samples2 = multiplyPattern?.find(exemplarFilename)?.let {
                        x ->
                    samples.map { "_x${x.groupValues.get(1)}_$it" }
                } ?: samples
                _CallsetDetailsPre(callsetId, it._2.filenames, samples2)
            }
        ).collect().sortedBy { it.callsetId }
        val filenameToCallsetId = callsetsDetailsPre.flatMapIndexed {
                i, it ->
            check(i == it.callsetId)
            it.filenames.map { fn -> fn to i }
        }.toMap()

        // collect samples from all callsets into an aggregated order, checking for duplicates
        val flatSamples = callsetsDetailsPre.flatMap {
            it.samples.map { sample -> sample to it.callsetId }
        }
        val samples = flatSamples.map { it.first }.distinct().sorted().toTypedArray()
        require(
            flatSamples.size == samples.size || allowDuplicateSamples,
            { "duplicate samples found among distinct VCF headers" }
        )
        val sampleIndex = samples.mapIndexed { i, sample -> sample to i }.toMap()

        // if allowDuplicateSamples, arbitrarily select one source callset for any sample that
        // appeared in multiple callsets
        val sampleCallset = flatSamples.toMap()

        // finalize CallsetsDetails by mapping callset sample names into the aggregated order
        val callsetsDetails = callsetsDetailsPre.map {
            val callsetSampleIndexes = it.samples.map {
                    sample ->
                if (sampleCallset.get(sample)!! == it.callsetId) sampleIndex.get(sample)!! else -1
            }.toIntArray()
            CallsetDetails(it.filenames, callsetSampleIndexes)
        }.toTypedArray()

        // collect & deduplicate the contig/FILTER/INFO/FORMAT lines
        var headerLines = filenamesByHeader.values().flatMap(
            FlatMapFunction<_FilenamesWithSameHeader, VcfHeaderLine> {
                getVcfHeaderLines(it.header).iterator()
            }
        ).distinct().collect()
        // There's something weird with VcfHeaderLineKind enum serialization causing
        // JavaRDD.distinct() to leave some duplicates when merging results from multiple JVMs. So
        // re-distinct() the in-memory list. Troubling....
        val headerLinesSize1 = headerLines.size
        headerLines = headerLines.distinct()
        val headerLinesSize2 = headerLines.size
        if (headerLinesSize1 != headerLinesSize2) {
            org.apache.log4j.LogManager.getLogger("GLnext").warn(
                "Deduplicated VcfHeaderLineKind left by Spark distinct()" +
                    " ($headerLinesSize1 > $headerLinesSize2)"
            )
        }
        val (headerLinesMap, contigs) = validateVcfHeaderLines(headerLines)
        val contigId = contigs.mapIndexed { ord, id -> id to ord.toShort() }.toMap()

        return AggVcfHeader(
            headerLinesMap,
            contigs,
            contigId,
            samples,
            filenameToCallsetId,
            callsetsDetails
        )
    } finally {
        filenamesByHeader.unpersist()
    }
}

fun callsetExemplarFilename(aggHeader: AggVcfHeader, callsetId: Int): String {
    return java.io.File(aggHeader.callsetsDetails[callsetId].callsetFilenames.first()).getName()!!
}

/**
 * Given deduplicated list of VcfHeaderLine, check them for internal consistency (ID uniqueness
 * etc.) and compile the contig ID mapping & global sample order
 */
fun validateVcfHeaderLines(headerLines: List<VcfHeaderLine>):
    Pair<VcfHeaderLineIndex, Array<String>> {
    // headerLines.sorted().forEach {
    //    println(it)
    // }
    require(headerLines.distinct().size == headerLines.size)
    // check for ID collisions/inconsistencies in header lines
    val headerLinesMap = headerLines.map { (it.kind to it.id) to it }.toMap()
    require(
        headerLines.size == headerLinesMap.size,
        {
            "inconsistent FILTER/INFO/FORMAT/contig header lines found in distinct VCF headers" +
                " (ID collision; ${headerLines.size} != ${headerLinesMap.size})"
        }
    )

    // compile contig list
    val contigLines = headerLines.filter { it.kind == VcfHeaderLineKind.CONTIG }.sortedBy { it.ord }
    require(
        contigLines.map { it.ord } == (0..(contigLines.size - 1)).toList(),
        { "inconsistent contigs found in distinct VCF headers" }
    )
    val contigs = contigLines.map { it.id }.toTypedArray()

    return headerLinesMap to contigs
}

/**
 * Read just the header from the VCF file and return (headerDigest, headerText) where headerDigest
 * is SHA-256 hex.
 */
fun readVcfHeader(filename: String, fs: FileSystem? = null): Pair<String, String> {
    var completeHeader = false
    val header = fileReaderDetectGz(filename, fs).useLines {
        return@useLines it.takeWhile { line ->
            require(line.length > 0, { "blank line in $filename" })
            line.startsWith('#').also { isHeaderLine -> completeHeader = !isHeaderLine }
        }.asSequence().joinToString(separator = "\n", postfix = "\n")
    }
    require(
        header.length > 0 && header.startsWith("##fileformat=VCF"),
        { "invalid VCF header in $filename" }
    )
    require(completeHeader, { "incomplete header or empty $filename" })
    val headerDigest = java.security.MessageDigest.getInstance("SHA-256")
        .digest(header.toByteArray())
        .map { "%02x".format(it) }
        .joinToString("")
    return headerDigest to header
}

/**
 * Parse VcfHeaderLines from complete header text
 */
fun getVcfHeaderLines(headerText: String): Sequence<VcfHeaderLine> {
    // use htsjdk's VCF header parser
    val headerLineIterator = HtsJdkLineIteratorImpl(
        headerText.trimEnd('\n').splitToSequence("\n").iterator()
    )
    val hdr = htsjdk.variant.vcf.VCFCodec().readActualHeader(headerLineIterator)
    check(hdr is htsjdk.variant.vcf.VCFHeader)
    return sequence {
        yieldAll(
            hdr.getContigLines().map {
                val seqRec = it.getSAMSequenceRecord()
                VcfHeaderLine(
                    VcfHeaderLineKind.CONTIG,
                    seqRec.getSequenceName(),
                    it.toString(),
                    it.getContigIndex()
                )
            }
        )
        yieldAll(
            hdr.getFilterLines().map {
                VcfHeaderLine(
                    VcfHeaderLineKind.FILTER,
                    it.getID(),
                    it.toString()
                )
            }
        )
        yieldAll(
            hdr.getInfoHeaderLines().map {
                VcfHeaderLine(
                    VcfHeaderLineKind.INFO,
                    it.getID(),
                    it.toString()
                )
            }
        )
        yieldAll(
            hdr.getFormatHeaderLines().map {
                VcfHeaderLine(
                    VcfHeaderLineKind.FORMAT,
                    it.getID(),
                    it.toString()
                )
            }
        )
    }
}

/**
 * Needed for htsjdk's VCF header parser
 */
class HtsJdkLineIteratorImpl(val inner: Iterator<String>) : htsjdk.tribble.readers.LineIterator {
    var peekedLine: String? = null

    override fun hasNext(): Boolean {
        return peekedLine != null || inner.hasNext()
    }

    override fun next(): String {
        peekedLine?.let {
            peekedLine = null
            return it
        }
        return inner.next()
    }

    override fun peek(): String {
        peekedLine?.let {
            return it
        } ?: run {
            val ans = inner.next()
            peekedLine = ans
            return ans
        }
    }

    override fun forEachRemaining(action: java.util.function.Consumer<in String>) {
        peekedLine?.let {
            peekedLine = null
            action.accept(it)
        }
        inner.forEachRemaining(action)
    }

    override fun remove() {
        throw UnsupportedOperationException()
    }
}

fun AggVcfHeader.vcfFilenamesDF(spark: SparkSession): Dataset<Row> {
    return spark.createDataFrame(
        JavaSparkContext(spark.sparkContext()).parallelizeEvenly(
            this.filenameCallsetId.toList()
        ).map { (filename, callsetId) ->
            RowFactory.create(filename, callsetId)
        },
        StructType()
            .add("vcfFilename", DataTypes.StringType, false)
            .add("callsetId", DataTypes.IntegerType, false)
    )
}
