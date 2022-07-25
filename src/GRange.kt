import org.apache.spark.sql.*
import org.apache.spark.sql.types.*

/**
 * Genomic range with reference sequence id (rid), one-based begin position, and inclusive end
 * position (matching the convention in the VCF text format).
 */
data class GRange(val rid: Short, val beg: Int, val end: Int) : Comparable<GRange>, java.io.Serializable {
    init {
        require(rid >= 0)
        require(beg > 0)
        require(end >= beg)
    }
    override fun compareTo(other: GRange) = compareValuesBy(this, other, { it.rid }, { it.beg }, { it.end })
    fun overlaps(other: GRange): Boolean = (rid == other.rid && beg <= other.end && end >= other.beg)
    fun contains(other: GRange): Boolean = (rid == other.rid && beg <= other.beg && end >= other.end)
    fun subtract(other: GRange): List<GRange> {
        if (!overlaps(other)) {
            return listOf(this)
        }
        var ans: List<GRange> = emptyList()
        if (beg < other.beg) {
            check(other.beg <= end)
            ans += GRange(rid, beg, other.beg - 1)
        }
        if (other.end < end) {
            check(beg <= other.end)
            ans += GRange(rid, other.end + 1, end)
        }
        return ans
    }
    fun subtract(others: List<GRange>): List<GRange> {
        var ans = listOf(this)
        others.forEach { other ->
            ans = ans.flatMap {
                it.subtract(other)
            }
        }
        return ans
    }
}
