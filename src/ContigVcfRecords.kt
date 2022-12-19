import kotlin.math.max
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.*
import org.apache.spark.sql.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.*
import org.apache.spark.util.LongAccumulator
import org.jetbrains.kotlinx.spark.api.*

/**
 * Batch of contiguous VCF records: ID of the source callset, bounding GRange, and text lines
 */
data class ContigVcfRecords(val callsetId: Int, val range: GRange, val lines: String) :
    java.io.Serializable {
    constructor(row: Row) :
        this(
            row.getAs<Int>("callsetId"),
            GRange(
                row.getAs<Short>("rid"),
                row.getAs<Int>("beg"),
                row.getAs<Int>("end")
            ),
            row.getAs<String>("lines")
        )
    fun toRow(): Row {
        return RowFactory.create(callsetId, range.rid, range.beg, range.end, lines)
    }
    fun records(contigId: Map<String, Short>): Sequence<VcfRecord> {
        return lines.splitToSequence("\n").map { parseVcfRecord(contigId, callsetId, it) }
    }
}

/**
 * StructType for VcfRecord.toRow()
 */
fun ContigVcfRecordsStructType(): StructType {
    return StructType()
        .add("callsetId", DataTypes.IntegerType, false)
        .add("rid", DataTypes.ShortType, false)
        .add("beg", DataTypes.IntegerType, false)
        .add("end", DataTypes.IntegerType, false)
        .add("lines", DataTypes.StringType, false)
}

/**
 * Read a complete VCF file to generate a sequence of ContigVcfRecords grouped by CHROM
 */
fun readOneVcfByContig(
    contigId: Map<String, Short>,
    callsetId: Int,
    filename: String,
    filterRanges: org.apache.spark.broadcast.Broadcast<BedRanges>?,
    andDelete: Boolean = false,
    recordCount: LongAccumulator? = null,
    recordBytes: LongAccumulator? = null,
    unfilteredRecordCount: LongAccumulator? = null
): Sequence<ContigVcfRecords> {
    return sequence {
        var rid: Short = -1
        var ridLines: MutableList<String> = mutableListOf()
        fun compileRidLines(): ContigVcfRecords {
            check(rid >= 0 && !ridLines.isEmpty())
            val range = GRange(rid, 1, 999999999) // TODO: real beg/end positions
            return ContigVcfRecords(callsetId, range, ridLines.joinToString("\n"))
        }
        val fs = getFileSystem(filename)
        fileReaderDetectGz(filename, fs).useLines {
            it.forEach {
                    line ->
                if (line.length > 0 && line.get(0) != '#') {
                    val tsv = line.splitToSequence('\t').take(VcfColumn.INFO.ordinal + 1)
                        .toList().toTypedArray()
                    val lineRange = parseVcfRecordRange(contigId, tsv)
                    if (lineRange.rid != rid && !ridLines.isEmpty()) {
                        yield(compileRidLines())
                        ridLines = mutableListOf()
                    }
                    rid = lineRange.rid
                    unfilteredRecordCount?.add(1L)
                    if (filterRanges?.let {
                            it.value!!.hasOverlapping(lineRange)
                        } ?: true
                    ) {
                        ridLines.add(line)
                        recordCount?.let { it.add(1L) }
                        recordBytes?.let { it.add(line.length + 1L) }
                    }
                }
            }
        }
        if (!ridLines.isEmpty()) {
            yield(compileRidLines())
        }
        if (andDelete) {
            fs.delete(Path(filename), false)
        }
    }
}

/**
 * Read many VCF files (+ aggregated header) into a DataFrame of ContigVcfRecords rows
 */
fun readAllVcfByContig(
    spark: SparkSession,
    aggHeader: AggVcfHeader,
    filterRanges: org.apache.spark.broadcast.Broadcast<BedRanges>?,
    andDelete: Boolean = false,
    recordCount: LongAccumulator? = null,
    recordBytes: LongAccumulator? = null,
    unfilteredRecordCount: LongAccumulator? = null
): Dataset<Row> {
    val jsc = JavaSparkContext(spark.sparkContext)

    val filenamesWithCallsetIds = jsc.parallelize(aggHeader.filenameCallsetId.toList())
        .repartition(max(jsc.defaultParallelism(), 4 * aggHeader.filenameCallsetId.size))
    val contigId = aggHeader.contigId
    // flatMap each filename+callsetId onto all ContigVcfRecords batches
    return spark.createDataFrame(
        filenamesWithCallsetIds.flatMap(
            FlatMapFunction<Pair<String, Int>, Row> {
                    p ->
                val (filename, callsetId) = p
                readOneVcfByContig(
                    contigId,
                    callsetId,
                    filename,
                    filterRanges = filterRanges,
                    andDelete = andDelete,
                    recordCount = recordCount,
                    recordBytes = recordBytes,
                    unfilteredRecordCount = unfilteredRecordCount
                )
                    .map { it.toRow() }.iterator()
            }
        ),
        ContigVcfRecordsStructType()
    )
}
