import org.apache.spark.sql.*
import org.jetbrains.kotlinx.spark.api.*

/**
 * Elementary variant
 */
data class Variant(val range: GRange, val ref: String, val alt: String) : Comparable<Variant> {
    override fun compareTo(other: Variant) = compareValuesBy(this, other, { it.range }, { it.alt })
    fun str(contigs: Array<String>): String {
        val contigName = contigs[range.rid.toInt()]
        return "$contigName:${range.beg}/$ref/$alt"
    }
}

/**
 * Harvest Variant from a VcfRecord
 */
fun VcfRecord.toVariant(): Variant {
    val tsv = line.splitToSequence("\t").take(5).toList().toTypedArray()
    val ref = tsv[3]
    val alt = tsv[4]
    check(ref.length == range.end - range.beg + 1)
    require(!alt.contains(',') && !alt.contains('.'))
    return Variant(range, ref, alt)
}

/**
 * Variant with one associated bin number
 */
data class BinnedVariant(val bin: Long, val variant: Variant) : Comparable<BinnedVariant> {
    override fun compareTo(other: BinnedVariant) = compareValuesBy(this, other, { it.bin }, { it.variant })
}

/**
 * In a dataset of BinnedVariant, variants whose range touches multiple bins are replicated for
 * each bin number.
 */
typealias BinnedVariants = Dataset<BinnedVariant>

/**
 * Harvest BinnedVariant from a BinnedVcfRecord (does not further replicate across bins)
 */
fun BinnedVcfRecord.toBinnedVariant(): BinnedVariant {
    return BinnedVariant(bin, record.toVariant())
}

/**
 * Harvest BinnedVariants from BinnedVcfRecords (mirroring the assumed replication in VcfRecords)
 */
fun BinnedVcfRecords.getBinnedVariants(): BinnedVariants {
    return map { it.toBinnedVariant() }.distinct()
}
