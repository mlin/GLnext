import org.apache.spark.sql.*
import org.apache.spark.sql.api.java.UDF4
import org.apache.spark.sql.types.*

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

/**
 * SparkSQL UDF for GRange(rid, beg, end).bins(binSize) -> long[]
 */
fun registerGRangeBinsUDF(spark: SparkSession) {
    spark.udf().register(
        "GRangeBins",
        object : UDF4<Short, Int, Int, Int, LongArray> {
            override fun call(rid: Short?, beg: Int?, end: Int?, binSize: Int?): LongArray {
                return GRange(rid!!, beg!!, end!!).bins(binSize!!).toList().toLongArray()
            }
        },
        ArrayType(DataTypes.LongType, false)
    )
}
