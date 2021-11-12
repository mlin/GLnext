/**
 * Genomic range with reference sequence id (rid), one-based begin position, and inclusive end
 * position (matching the convention in the VCF text format).
 */
data class GRange(val rid: Short, val beg: Int, val end: Int) : Comparable<GRange> {
    init {
        require(rid >= 0)
        require(beg > 0)
        require(end >= beg)
    }
    override fun compareTo(other: GRange) = compareValuesBy(this, other, { it.rid }, { it.beg }, { it.end })
    fun overlaps(other: GRange): Boolean {
        return rid == other.rid && beg <= other.end && end >= other.beg
    }
    /**
     * Calculate bin number(s) for a GRange. A range may touch multiple bins.
     */
    fun bins(binSize: Int): LongRange {
        val ridBits: Long = rid.toLong() shl 48
        val firstBin = beg.toLong() / binSize.toLong()
        val lastBin = end.toLong() / binSize.toLong()
        check(0 <= firstBin && firstBin <= lastBin && lastBin < (1L shl 48))
        return LongRange(ridBits + firstBin, ridBits + lastBin)
    }
}
