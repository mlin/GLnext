import org.apache.spark.sql.*
import org.apache.spark.util.LongAccumulator
import org.jetbrains.kotlinx.spark.api.*

/**
 * Raw VCF record: ID of the source callset, extracted GRange, and Snappy-compressed original line
 */
data class VcfRecord(val callsetId: Int, val range: GRange, val line: String) : Comparable<VcfRecord> {
    override fun compareTo(other: VcfRecord) = compareValuesBy(this, other, { callsetId }, { range }, { line })
    /* snappyLine: ByteArray
       disabled due to: https://github.com/JetBrains/kotlin-spark-api/issues/111
    fun uncompressLine(): String {
        return String(Snappy.uncompress(snappyLine))
    }*/
}

/**
 * Spark dataset of VcfRecord
 */
typealias VcfRecords = Dataset<VcfRecord>

/**
 * Load VcfRecords dataset from a bunch of files (parallelized)
 */
fun readVcfRecords(
    spark: org.apache.spark.sql.SparkSession,
    aggHeader: AggVcfHeader,
    deleteInputVcfs: Boolean = false,
    recordCount: LongAccumulator? = null
): VcfRecords {
    // flatMap the records from each file, each with an associated callsetId
    var filenamesWithCallsetIds = spark.toDS(aggHeader.filenameCallsetId.toList())
    // pigeonhole partitions (assume each file is a reasonable size)
    filenamesWithCallsetIds = filenamesWithCallsetIds.repartitionByRange(
        aggHeader.filenameCallsetId.size,
        filenamesWithCallsetIds.col(filenamesWithCallsetIds.columns()[0])
    )
    val contigId = aggHeader.contigId
    return filenamesWithCallsetIds.flatMap {
        (filename, callsetId) ->
        sequence {
            vcfInputStream(filename).bufferedReader().useLines {
                it.forEach {
                    line ->
                    if (line.length > 0 && line.get(0) != '#') {
                        val tsv = line.splitToSequence("\t").take(8).toList().toTypedArray()
                        val rid = contigId.getOrDefault(tsv[0], -1)
                        require(rid >= 0, { "unknown CHROM ${tsv[0]} in $filename" })
                        val beg = tsv[1].toInt()
                        // FIXME: use END INFO if applicable
                        val end = beg + tsv[3].length - 1
                        val range = GRange(rid.toShort(), beg, end)
                        // val snappyLine = Snappy.compress(line.toByteArray())
                        recordCount?.let { it.add(1L) }
                        yield(VcfRecord(callsetId, range, line)) // snappyLine
                    }
                }
            }
            if (deleteInputVcfs) {
                java.io.File(filename).delete()
            }
        }.iterator()
    }
}

/**
 * A VCF record one associated bin number.
 */
data class BinnedVcfRecord(val bin: Long, val record: VcfRecord) : Comparable<BinnedVcfRecord> {
    override fun compareTo(other: BinnedVcfRecord) = compareValuesBy(this, other, { it.bin }, { it.record })
}

/**
 * In a dataset of BinnedVcfRecord, records whose range touches multiple bins are replicated for
 * each bin number.
 */
typealias BinnedVcfRecords = Dataset<BinnedVcfRecord>

/**
 * Compute the BinnedVcfRecord(s) for a VcfRecord (replicating for multiple bins as needed)
 */
fun VcfRecord.binned(binSize: Int, binnedRecordCount: LongAccumulator? = null): Sequence<BinnedVcfRecord> {
    val bins = range.bins(binSize)
    binnedRecordCount?.let { it.add(bins.count().toLong()) }
    return bins.asSequence().map { BinnedVcfRecord(it, this) }
}

/**
 * Bin the VcfRecords dataset (with counts of input records & binned records)
 */
fun VcfRecords.binned(binSize: Int, binnedRecordCount: LongAccumulator): BinnedVcfRecords {
    return flatMap {
        it.binned(binSize, binnedRecordCount).iterator()
    }
}
