import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.math.max
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.*
import org.apache.spark.sql.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.*
import org.apache.spark.util.LongAccumulator
import org.jetbrains.kotlinx.spark.api.*
import org.xerial.snappy.SnappyInputStream
import org.xerial.snappy.SnappyOutputStream

/**
 * Batch of contiguous VCF records: ID of the source callset, bounding GRange, and text lines
 */
data class ContigVcfRecords(val callsetId: Int, val range: GRange, val snappyLines: ByteArray) :
    java.io.Serializable {
    constructor(row: Row) :
        this(
            row.getAs<Int>("callsetId"),
            GRange(
                row.getAs<Short>("rid"),
                row.getAs<Int>("beg"),
                row.getAs<Int>("end")
            ),
            row.getAs<ByteArray>("snappyLines")
        )
    fun toRow(): Row {
        return RowFactory.create(callsetId, range.rid, range.beg, range.end, snappyLines)
    }
    fun records(contigId: Map<String, Short>): Sequence<VcfRecord> {
        return sequence {
            SnappyInputStream(ByteArrayInputStream(snappyLines))
                .reader().buffered().use { reader ->
                    var line = reader.readLine()
                    while (line != null) {
                        yield(parseVcfRecord(contigId, callsetId, line))
                        line = reader.readLine()
                    }
                }
        }
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
        .add("snappyLines", DataTypes.BinaryType, false)
}

/**
 * Read a complete VCF file to generate a sequence of ContigVcfRecords for each range in ranges.
 * If ranges is omitted, then one ContigVcfRecords per CHROM.
 */
fun readContigVcfRecords(
    contigId: Map<String, Short>,
    callsetId: Int,
    filename: String,
    ranges: GRangeIndex<GRange>?,
    andDelete: Boolean = false,
    unfilteredRecordCount: LongAccumulator? = null,
    recordCount: LongAccumulator? = null,
    recordBytes: LongAccumulator? = null
): Sequence<ContigVcfRecords> {
    val effRanges = ranges ?: GRangeIndex<GRange>(
        contigId.values.map {
            GRange(it, 1, Int.MAX_VALUE)
        }.iterator(),
        { it }
    )
    val recordStream = sequence {
        val fs = getFileSystem(filename)
        fileReaderDetectGz(filename, fs).useLines {
            it.forEach { line ->
                if (line.length > 0 && line.get(0) != '#') {
                    val tsv = line.splitToSequence('\t').take(VcfColumn.INFO.ordinal + 1)
                        .toList().toTypedArray()
                    val lineRange = parseVcfRecordRange(contigId, tsv)
                    unfilteredRecordCount?.add(1L)
                    yield(lineRange to line)
                }
            }
        }
        if (andDelete) {
            fs.delete(Path(filename), false)
        }
    }
    return sequence {
        effRanges.streamingJoin(recordStream.iterator(), { it.first }).forEach { (rangeId, lines) ->
            val bos = ByteArrayOutputStream()
            SnappyOutputStream(bos, 262144).writer().use { snappy ->
                lines.forEach { (_, line) ->
                    recordCount?.let { it.add(1L) }
                    recordBytes?.let { it.add(line.length + 1L) }
                    snappy.write(line)
                    snappy.write(10)
                }
            }
            yield(
                ContigVcfRecords(
                    callsetId,
                    effRanges.item(rangeId),
                    bos.toByteArray()
                )
            )
        }
    }
}

/**
 * Read many VCF files (+ aggregated header) into a DataFrame of ContigVcfRecords rows
 */
fun readAllContigVcfRecords(
    spark: SparkSession,
    aggHeader: AggVcfHeader,
    ranges: org.apache.spark.broadcast.Broadcast<GRangeIndex<GRange>>?,
    andDelete: Boolean = false,
    unfilteredRecordCount: LongAccumulator? = null,
    recordBatchCount: LongAccumulator? = null,
    recordCount: LongAccumulator? = null,
    recordBytes: LongAccumulator? = null
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
                readContigVcfRecords(
                    contigId,
                    callsetId,
                    filename,
                    ranges = ranges?.value,
                    andDelete = andDelete,
                    unfilteredRecordCount = unfilteredRecordCount,
                    recordCount = recordCount,
                    recordBytes = recordBytes
                ).map {
                    recordBatchCount?.let { it.add(1L) }
                    it.toRow()
                }.iterator()
            }
        ),
        ContigVcfRecordsStructType()
    )
}
