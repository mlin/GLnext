import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.*
import org.apache.spark.util.LongAccumulator
import org.jetbrains.kotlinx.spark.api.*
import org.xerial.snappy.Snappy

enum class VcfColumn {
    CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO, FORMAT, FIRST_SAMPLE
}

/**
 * Raw VCF record: ID of the source callset, extracted GRange, and original line
 */
data class VcfRecord(val callsetId: Int, val range: GRange, val line: String) {
    fun toRow(compress: Boolean = false): Row {
        if (compress) {
            val snappyLine = Snappy.compress(line.toByteArray())
            return RowFactory.create(callsetId, range.rid, range.beg, range.end, snappyLine)
        } else {
            return RowFactory.create(callsetId, range.rid, range.beg, range.end, line)
        }
    }
}

/**
 * RowEncoder for VcfRecord.toRow()
 */
fun VcfRecordRowEncoder(compress: Boolean = false): ExpressionEncoder<Row> {
    var ty = StructType()
        .add("callsetId", DataTypes.IntegerType, false)
        .add("rid", DataTypes.ShortType, false)
        .add("beg", DataTypes.IntegerType, false)
        .add("end", DataTypes.IntegerType, false)
    if (compress) {
        ty = ty.add("snappyLine", DataTypes.BinaryType, false)
    } else {
        ty = ty.add("line", DataTypes.StringType, false)
    }
    return RowEncoder.apply(ty)
}

/**
 * Parse VCF text line into VcfRecord
 */
fun parseVcfRecord(contigId: Map<String, Short>, callsetId: Int, line: String): VcfRecord {
    val tsv = line.splitToSequence('\t').take(VcfColumn.INFO.ordinal + 1).toList().toTypedArray()
    return VcfRecord(callsetId, parseVcfRecordRange(contigId, tsv), line)
}

fun parseVcfRecordRange(contigId: Map<String, Short>, tsv: Array<String>): GRange {
    val rid = contigId.getOrDefault(tsv[VcfColumn.CHROM.ordinal], -1)
    require(rid >= 0, { "unknown CHROM ${tsv[0]}" })
    val beg = tsv[VcfColumn.POS.ordinal].toInt()
    val endRef = beg + tsv[VcfColumn.REF.ordinal].length - 1
    val endInfo = parseVcfRecordInfo(tsv[VcfColumn.INFO.ordinal]).get("END")?.toInt()
    if (endInfo != null) {
        require(endInfo >= beg, { "invalid VCF INFO END $endInfo (< POS $endRef)" })
        return GRange(rid, beg, endInfo)
    }
    return GRange(rid, beg, endRef)
}

fun parseVcfRecordInfo(info: String): Map<String, String> {
    return hashMapOf(
        *info.split(';').map {
            val kv = it.split('=', limit = 2)
            check(kv.size == 1 || kv.size == 2)
            kv[0] to (if (kv.size > 1) kv[1] else "")
        }.toTypedArray()
    )
}

/**
 * From multiple VCF files (with aggregated header), construct DataFrame of VcfRecord Rows
 */
fun readVcfRecordsDF(
    spark: SparkSession,
    aggHeader: AggVcfHeader,
    compressEachRecord: Boolean = false,
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
                                yield(parseVcfRecord(contigId, callsetId, line).toRow(compressEachRecord))
                            }
                        }
                    }
                    if (deleteInputVcfs) {
                        java.io.File(filename).delete()
                    }
                }.iterator()
            }
        },
        VcfRecordRowEncoder(compressEachRecord)
    )
}
