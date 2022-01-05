import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.*
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.*
import org.jetbrains.kotlinx.spark.api.*
import org.xerial.snappy.Snappy

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
 * Harvest Variant from a VcfRecord
 */
fun VcfRecord.toVariant(): Variant {
    val tsv = line.splitToSequence('\t').take(5).toList().toTypedArray()
    val ref = tsv[3]
    val alt = tsv[4]
    check(ref.length == range.end - range.beg + 1)
    require(!alt.contains(',') && !alt.contains('.'))
    return Variant(range, ref, alt)
}

/**
 * Harvest all distinct Variants from VcfRecord DataFrame
 */
fun discoverVariants(vcfRecordsDF: Dataset<Row>): Dataset<Row> {
    return vcfRecordsDF
        .map(
            object : MapFunction<Row, Row> {
                override fun call(row: Row): Row {
                    val range = GRange(row.getAs<Short>("rid"), row.getAs<Int>("beg"), row.getAs<Int>("end"))
                    val vcfRecord = VcfRecord(
                        row.getAs<Int>("callsetId"), range,
                        String(Snappy.uncompress(row.getAs<ByteArray>("snappyLine")))
                    )
                    return vcfRecord.toVariant().toRow()
                }
            },
            VariantRowEncoder()
        ).distinct()
}
