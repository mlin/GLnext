/**
 * Tools for reading VCF file headers and quickly aggregating many of them (e.g. 1M+ GVCFs)
 *
 * Also contemplates that one logical "callset" might materialize as several VCF files (e.g. one
 * file per chromosome). The assumption is that files with identical headers (including the sample
 * identifier(s) on the #CHROM line) comprise one callset.
 */
import org.apache.spark.sql.*
import org.jetbrains.kotlinx.spark.api.*
import org.apache.hadoop.fs.FileSystem as Hdfs

/**
 * VCF header contig/FILTER/INFO/FORMAT lines
 */
enum class VcfHeaderLineKind { FILTER, INFO, FORMAT, CONTIG }
data class VcfHeaderLine(val kind: VcfHeaderLineKind, val id: String, val lineText: String, val ord: Int = -1) :
    java.io.Serializable, Comparable<VcfHeaderLine> {
    override fun compareTo(other: VcfHeaderLine) = compareValuesBy(this, other, { it.kind }, { it.ord }, { it.id })
}
typealias VcfHeaderLineIndex = Map<Pair<VcfHeaderLineKind, String>, VcfHeaderLine>

/**
 * Details specific to one callset: for each sample in the original callset, its index in the
 * aggregated sample list
 */
data class CallsetDetails(val callsetFilenames: List<String>, val callsetSamples: IntArray) : java.io.Serializable

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
) : java.io.Serializable

/**
 * Use Spark to load all VCF headers and aggregate them
 */
fun aggregateVcfHeaders(spark: org.apache.spark.sql.SparkSession, filenames: List<String>, allowDuplicateSamples: Boolean = false, hdfs: Hdfs? = null): AggVcfHeader {
    spark.toDS(filenames)
        // read & digest all VCF headers
        .map {
            val (headerDigest, header) = readVcfHeader(it, hdfs)
            Triple(it, headerDigest, header)
        }
        // Group headers by callset (header digest) & consolidate filename list for each
        .groupByKey { it.second }
        .mapGroups {
            headerDigest, items ->
            var header: String? = null
            val callsetFilenames = items.map {
                if (header == null) {
                    header = it.third
                }
                it.first
            }.asSequence().toList().sorted()
            Triple(headerDigest, header!!, callsetFilenames)
        }.withCached {
            // assign callsetIds as indexes into an array of distinct digests (arbitrary order)
            val digests: Array<String> = map { it.first }.toArray()
            val digestToCallsetId = sparkSession().sparkContext.broadcast(
                digests.sorted().mapIndexed { callsetId, digest -> digest to callsetId }.toMap()
            )

            // collect CallsetDetails (precursor)
            val callsetsDetailsPre = map {
                (digest, headerText, filenames) ->
                val callsetId = digestToCallsetId.value.get(digest)!!
                // read samples from last line of header
                val lastHeaderLine = headerText.trimEnd('\n').splitToSequence("\n").last()
                require(lastHeaderLine.startsWith("#CHROM"))
                val samples = lastHeaderLine.splitToSequence("\t").drop(9).toList()
                require(samples.toSet().size == samples.size, {
                    val exemplarFilename = java.io.File(filenames.first()).getName()!!
                    "$exemplarFilename header has duplicate sample names"
                })
                callsetId to (filenames to samples)
            }.collectAsList().sortedBy { it.first }.mapIndexed {
                i, it ->
                check(i == it.first)
                it.second
            }
            val filenameToCallsetId = callsetsDetailsPre.flatMapIndexed {
                i, (filenames, _) ->
                filenames.map { it to i }
            }.toMap()

            // collect samples from all callsets into an aggregated order, checking for duplicates
            val flatSamples = callsetsDetailsPre.mapIndexed {
                callsetId, it ->
                it.second.map { sample -> sample to callsetId }
            }.flatten()
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
            val callsetsDetails = callsetsDetailsPre.mapIndexed {
                callsetId, (callsetFilenames, callsetSamples) ->
                val callsetSampleIndexes = callsetSamples.map {
                    if (sampleCallset.get(it)!! == callsetId) sampleIndex.get(it)!! else -1
                }.toIntArray()
                CallsetDetails(callsetFilenames, callsetSampleIndexes)
            }.toTypedArray()

            // collect & deduplicate the contig/FILTER/INFO/FORMAT lines
            val headerLines = flatMap {
                (_, headerText, _) ->
                getVcfHeaderLines(headerText).iterator()
            }.distinct().collectAsList()
            val (headerLinesMap, contigs) = validateVcfHeaderLines(headerLines)
            val contigId = contigs.mapIndexed { ord, id -> id to ord.toShort() }.toMap()

            return AggVcfHeader(headerLinesMap, contigs, contigId, samples, filenameToCallsetId, callsetsDetails)
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
    // check for ID collisions/inconsistencies in header lines
    val headerLinesMap = headerLines.map { (it.kind to it.id) to it }.toMap()
    require(
        headerLines.size == headerLinesMap.size,
        { "inconsistent FILTER/INFO/FORMAT/contig header lines found in distinct VCF headers (ID collision)" }
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
fun readVcfHeader(filename: String, hdfs: Hdfs? = null): Pair<String, String> {
    val header = vcfInputStream(filename, hdfs).bufferedReader().useLines {
        return@useLines it.takeWhile { it.length > 0 && it.get(0) == '#' }
            .asSequence()
            .joinToString(separator = "\n", postfix = "\n")
    }
    require(header.length > 0 && header.startsWith("##fileformat=VCF"), { "invalid VCF header in $filename" })
    val headerDigest = java.security.MessageDigest.getInstance("SHA-256")
        .digest(header.toByteArray())
        .map { "%02x".format(it) }
        .joinToString("")
    return headerDigest to header
}

/**
 * Open file InputStream, gunzipping if applicable
 */
fun vcfInputStream(filename: String, hdfs: Hdfs? = null): java.io.InputStream {
    var instream: java.io.InputStream
    if (filename.startsWith("hdfs:")) {
        check(hdfs != null)
        instream = hdfs.open(org.apache.hadoop.fs.Path(filename.substring(5)))
    } else {
        instream = java.io.File(filename).inputStream()
    }
    if (filename.endsWith(".gz")) {
        instream = java.util.zip.GZIPInputStream(instream)
    }
    return instream
}

/**
 * Parse VcfHeaderLines from complete header text
 */
fun getVcfHeaderLines(headerText: String): Sequence<VcfHeaderLine> {
    // use htsjdk's VCF header parser
    val headerLineIterator = HtsJdkLineIteratorImpl(headerText.trimEnd('\n').splitToSequence("\n").iterator())
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
