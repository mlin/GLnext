import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.*
import org.apache.spark.util.LongAccumulator
import org.jetbrains.kotlinx.spark.api.*
import org.xerial.snappy.Snappy

/**
 * Raw VCF record: ID of the source callset, extracted GRange, and original line
 */
data class VcfRecord(val callsetId: Int, val range: GRange, val line: String) {
    fun toRow(): Row {
        // row serialization: Snappy-compress the line text
        val snappyLine = Snappy.compress(line.toByteArray())
        return RowFactory.create(callsetId, range.rid, range.beg, range.end, snappyLine)
    }
}

/**
 * RowEncoder for VcfRecord.toRow()
 */
fun VcfRecordRowEncoder(): ExpressionEncoder<Row> {
    return RowEncoder.apply(
        StructType()
            .add("callsetId", DataTypes.IntegerType, false)
            .add("rid", DataTypes.ShortType, false)
            .add("beg", DataTypes.IntegerType, false)
            .add("end", DataTypes.IntegerType, false)
            .add("snappyLine", DataTypes.BinaryType, false)
    )
}

/**
 * Parse VCF text line into VcfRecord
 */
fun parseVcfRecord(contigId: Map<String, Short>, callsetId: Int, line: String): VcfRecord {
    val tsv = line.splitToSequence('\t').take(8).toList().toTypedArray()
    val rid = contigId.getOrDefault(tsv[0], -1)
    require(rid >= 0, { "unknown CHROM ${tsv[0]}" })
    val beg = tsv[1].toInt()
    // FIXME: use END INFO if applicable
    val end = beg + tsv[3].length - 1
    return VcfRecord(callsetId, GRange(rid, beg, end), line)
}

/**
 * From multiple VCF files (with aggregated header), construct DataFrame of VcfRecord Rows
 */
fun readVcfRecordsDF(
    spark: SparkSession,
    aggHeader: AggVcfHeader,
    deleteInputVcfs: Boolean = false,
    recordCount: LongAccumulator? = null,
    recordBytes: LongAccumulator? = null
): Dataset<Row> {
    var filenamesWithCallsetIds = spark.toDS(aggHeader.filenameCallsetId.toList())
    // pigeonhole partitions (assume each file is a reasonable size)
    filenamesWithCallsetIds = filenamesWithCallsetIds.repartitionByRange(
        aggHeader.filenameCallsetId.size,
        filenamesWithCallsetIds.col(filenamesWithCallsetIds.columns()[0])
    )
    val contigId = aggHeader.contigId
    // flatMap each filename+callsetId onto all the records in the file
    return filenamesWithCallsetIds.flatMap(
        object : FlatMapFunction<Pair<String, Int>, Row> {
            override fun call(p: Pair<String, Int>): Iterator<Row> {
                val (filename, callsetId) = p
                return sequence {
                    vcfInputStream(filename).bufferedReader().useLines {
                        it.forEach {
                            line ->
                            if (line.length > 0 && line.get(0) != '#') {
                                recordCount?.let { it.add(1L) }
                                recordBytes?.let { it.add(line.length + 1L) }
                                yield(parseVcfRecord(contigId, callsetId, line).toRow())
                            }
                        }
                    }
                    if (deleteInputVcfs) {
                        java.io.File(filename).delete()
                    }
                }.iterator()
            }
        },
        VcfRecordRowEncoder()
    )
}
