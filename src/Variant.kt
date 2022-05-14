import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.*
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.*
import org.jetbrains.kotlinx.spark.api.*
import org.xerial.snappy.Snappy
import kotlin.math.min

/**
 * Elementary variant
 */
data class Variant(val range: GRange, val ref: String, val alt: String) {
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
 * Generate normalized Variant for each ALT allele in the record; null for symbolic alleles
 */
fun VcfRecord.altsToVariants(): Array<Variant?> {
    val tsv = line.splitToSequence('\t').take(VcfColumn.ALT.ordinal + 1).toList().toTypedArray()
    val ref = tsv[VcfColumn.REF.ordinal]
    val alts = tsv[VcfColumn.ALT.ordinal].split(',')

    return alts.map { alt ->
        if (alt == "." || alt == "*" || alt.startsWith('<')) {
            null
        } else {
            require(
                ref.length == range.end - range.beg + 1 && !alt.contains('.') && !alt.contains('<'),
                { "invalid variant ${tsv.joinToString("\t")} (END=${range.end})" }
            )
            Variant(range, ref, alt).normalize()
        }
    }.toTypedArray()
}

/**
 * Get the non-null normalized Variants
 */
fun VcfRecord.toVariants(): List<Variant> {
    return this.altsToVariants().filterNotNull()
}

/**
 * Normalize variant by trimming reference padding
 * TODO: full, left-aligning normalization algo
 */
fun Variant.normalize(): Variant {
    var trimR = 0
    while (trimR < min(ref.length, alt.length) - 1 && ref[ref.length - trimR - 1] == alt[alt.length - trimR - 1]) {
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
 */
fun discoverVariants(vcfRecordsDF: Dataset<Row>): Dataset<Row> {
    val vcfRecordsCompressed = vcfRecordsDF.columns().contains("snappyLine")
    return vcfRecordsDF
        .flatMap(
            object : FlatMapFunction<Row, Row> {
                override fun call(row: Row): Iterator<Row> {
                    val range = GRange(row.getAs<Short>("rid"), row.getAs<Int>("beg"), row.getAs<Int>("end"))
                    val vcfRecord = VcfRecord(
                        row.getAs<Int>("callsetId"), range,
                        if (vcfRecordsCompressed) {
                            String(Snappy.uncompress(row.getAs<ByteArray>("snappyLine")))
                        } else {
                            row.getAs<String>("line")
                        }
                    )
                    return vcfRecord.toVariants().map { it.toRow() }.iterator()
                }
            },
            VariantRowEncoder()
        ).distinct()
    // TODO: accumulate instead of distinct() to collect summary stats
}
