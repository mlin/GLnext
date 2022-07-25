import kotlin.math.min
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.*
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.*
import org.jetbrains.kotlinx.spark.api.*
import org.xerial.snappy.Snappy

/**
 * Elementary variant
 */
data class Variant(val range: GRange, val ref: String, val alt: String) :
    Comparable<Variant>, java.io.Serializable {
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
    override fun compareTo(other: Variant) = compareValuesBy(
        this, other, { it.range }, { it.ref }, { it.alt }
    )
    fun str(contigs: Array<String>): String {
        val contigName = contigs[range.rid.toInt()]
        return "$contigName:${range.beg}/$ref/$alt"
    }
    fun toRow(): Row {
        return RowFactory.create(range.rid, range.beg, range.end, ref, alt)
    }
}

/**
 * RowEncoder for Variant.toRow()
 */
fun VariantRowEncoder(): ExpressionEncoder<Row> {
    return RowEncoder.apply(
        StructType()
            .add("rid", DataTypes.ShortType, false)
            .add("beg", DataTypes.IntegerType, false)
            .add("end", DataTypes.IntegerType, false)
            .add("ref", DataTypes.StringType, false)
            .add("alt", DataTypes.StringType, false)
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

/**
 * Harvest all distinct Variants from VcfRecord DataFrame
 * onlyCalled: only include variants with at least one copy called in a sample GT
 */
fun discoverVariants(vcfRecordsDF: Dataset<Row>, onlyCalled: Boolean = false): Dataset<Row> {
    val vcfRecordsCompressed = vcfRecordsDF.columns().contains("snappyLine")
    return vcfRecordsDF
        .flatMap(
            FlatMapFunction<Row, Row> {
                row ->
                val range = GRange(
                    row.getAs<Short>("rid"),
                    row.getAs<Int>("beg"),
                    row.getAs<Int>("end")
                )
                val vcfRecord = VcfRecordUnpacked(
                    VcfRecord(
                        row.getAs<Int>("callsetId"), range,
                        if (vcfRecordsCompressed) {
                            String(Snappy.uncompress(row.getAs<ByteArray>("snappyLine")))
                        } else {
                            row.getAs<String>("line")
                        }
                    )
                )
                val variants = vcfRecord.altVariants.copyOf()
                if (!onlyCalled) {
                    variants.filterNotNull().map { it.toRow() }.iterator()
                } else {
                    val copies = variants.map { 0 }.toTypedArray()
                    for (sampleIndex in 0 until vcfRecord.sampleCount) {
                        val gt = vcfRecord.getDiploidGenotype(sampleIndex)
                        if (gt.allele1 != null && gt.allele1 > 0) {
                            copies[gt.allele1 - 1]++
                        }
                        if (gt.allele2 != null && gt.allele2 > 0) {
                            copies[gt.allele2 - 1]++
                        }
                    }
                    variants.filterIndexed { i, _ -> copies[i] > 0 }.filterNotNull()
                        .map { it.toRow() }.iterator()
                }
            },
            VariantRowEncoder()
        ).distinct() // TODO: accumulate instead of distinct() to collect summary stats
        .selectExpr("*", "monotonically_increasing_id() as vid")
}
