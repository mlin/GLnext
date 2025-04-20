package net.mlin.GLnext.data
import kotlin.math.min
import net.mlin.GLnext.util.getIntOrNull
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

/**
 * Elementary variant
 */
data class Variant(val range: GRange, val ref: String, val alt: String) :
    Comparable<Variant> {
    constructor(row: Row) :
        this(
            GRange(
                row.getAs<Short>("rid"),
                row.getAs<Int>("beg"),
                row.getAs<Int>("end")
            ),
            row.getAs<String>("ref"),
            row.getAs<String>("alt")
        )
    constructor(rs: java.sql.ResultSet) :
        this(
            GRange(
                rs.getShort("rid"),
                rs.getInt("beg") + 1,
                rs.getInt("end")
            ),
            rs.getString("ref"),
            rs.getString("alt")
        )
    override fun compareTo(other: Variant) = compareValuesBy(
        this,
        other,
        { it.range },
        { it.ref },
        { it.alt }
    )
    fun str(contigs: Array<String>): String {
        val contigName = contigs[range.rid.toInt()]
        return "$contigName:${range.beg}/$ref/$alt"
    }
    fun toRow(copies: Int = 0, qual: Int? = null): Row {
        return RowFactory.create(range.rid, range.beg, range.end, ref, alt, copies, qual)
    }
}

/**
 * RowEncoder for Variant.toRow()
 */
fun VariantRowEncoder(): Encoder<Row> {
    return Encoders.row(
        StructType()
            .add("rid", DataTypes.ShortType, false)
            .add("beg", DataTypes.IntegerType, false)
            .add("end", DataTypes.IntegerType, false)
            .add("ref", DataTypes.StringType, false)
            .add("alt", DataTypes.StringType, false)
            .add("copies", DataTypes.IntegerType, false)
            .add("qual", DataTypes.IntegerType, true)
    )
}

/**
 * Normalize variant by trimming reference padding
 * TODO: full, left-aligning normalization algo
 */
fun Variant.normalize(): Variant {
    var trimR = 0
    while (trimR < min(ref.length, alt.length) - 1 &&
        ref[ref.length - trimR - 1] == alt[alt.length - trimR - 1]
    ) {
        trimR++
    }
    var trimL = 0
    while (trimL < min(ref.length, alt.length) - trimR - 1 && ref[trimL] == alt[trimL]) {
        trimL++
    }
    if (trimL == 0 && trimR == 0) {
        return this
    }
    val normRange = GRange(range.rid, range.beg + trimL, range.end - trimR)
    val normRef = ref.substring(trimL, ref.length - trimR)
    val normAlt = alt.substring(trimL, alt.length - trimR)
    check(
        normRef.length > 0 && normAlt.length > 0 &&
            normRef.length == normRange.end - normRange.beg + 1
    )
    return Variant(normRange, normRef, normAlt)
}

// Statistics aggregated in variant discovery
data class VariantStats(val copies: Int, val qual: Int?, val qual2: Int?) {
    constructor(row: Row) :
        this(
            row.getAs<Int>("copies"),
            row.getAs<Int?>("qual"),
            row.getAs<Int?>("qual2")
        )
    constructor(rs: java.sql.ResultSet) :
        this(
            rs.getInt("copies"),
            rs.getIntOrNull("qual"),
            rs.getIntOrNull("qual2")
        )
}
