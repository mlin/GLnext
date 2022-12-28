import java.io.File
import java.sql.*
import net.mlin.genomicsqlite.GenomicSQLite
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.*
import org.apache.spark.sql.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.*
import org.apache.spark.util.LongAccumulator
import org.jetbrains.kotlinx.spark.api.*

/**
 * Load a VCF file into a compressed database file (only the records overlapping filterRanges)
 */
fun loadVcfRecordDb(
    contigId: Map<String, Short>,
    filename: String,
    filterRanges: GRangeIndex<GRange>?,
    andDelete: Boolean = false,
    unfilteredRecordCount: LongAccumulator? = null,
    recordCount: LongAccumulator? = null,
    recordBytes: LongAccumulator? = null
): String {
    val fs = getFileSystem(filename)
    val tempFile = File.createTempFile("VcfRecord.", ".db")
    val tempFilename = tempFile.absolutePath
    tempFile.delete()

    var count = 0L
    var bytes = 0L
    ExitStack().use { cleanup ->
        val dbc = cleanup.add(createGenomicSQLiteForBulkLoad(tempFilename))
        val adhoc = cleanup.add(dbc.createStatement())
        adhoc.executeUpdate(
            """
            CREATE TABLE VcfRecord(
                rid INTEGER NOT NULL,
                beg INTEGER NOT NULL,
                end INTEGER NOT NULL,
                line TEXT NOT NULL
            )
            """
        )
        val insert = cleanup.add(dbc.prepareStatement("INSERT INTO VcfRecord VALUES(?,?,?,?)"))
        fileReaderDetectGz(filename, fs).useLines {
            it.forEach { line ->
                if (line.length > 0 && line.get(0) != '#') {
                    val tsv = line.splitToSequence('\t').take(VcfColumn.INFO.ordinal + 1)
                        .toList().toTypedArray()
                    val lineRange = parseVcfRecordRange(contigId, tsv)
                    unfilteredRecordCount?.add(1L)
                    if (filterRanges?.hasOverlapping(lineRange) ?: true) {
                        insert.setInt(1, lineRange.rid.toInt())
                        insert.setInt(2, lineRange.beg - 1)
                        insert.setInt(3, lineRange.end)
                        insert.setString(4, line)
                        // TODO: could strip CHROM, POS, END from line since they're now redundant
                        insert.executeUpdate()
                        count += 1L
                        bytes += line.length.toLong()
                    }
                }
            }
        }
        adhoc.executeUpdate(
            GenomicSQLite.createGenomicRangeIndexSQL(
                dbc,
                "VcfRecord",
                "rid",
                "beg",
                "end"
            )
        )
        dbc.commit()
    }
    /*
    // integrity check
    openGenomicSQLiteReadOnly(tempFilename).use { dbc ->
        dbc.createStatement().use() { stmt ->
            val rs = stmt.executeQuery(
                "SELECT count(*) AS count, sum(length(line)) AS bytes FROM VcfRecord"
            )
            check(rs.getLong("count") == count)
            check(rs.getLong("bytes") == bytes)
        }
    }
    */
    recordCount?.let { it.add(count) }
    recordBytes?.let { it.add(bytes + count) }

    val crc = java.util.zip.CRC32C()
    tempFile.inputStream().use {
        val buf = ByteArray(1048576)
        var n = it.read(buf)
        while (n >= 0) {
            crc.update(buf, 0, n)
            n = it.read(buf)
        }
    }
    val storedFilename = filename + ".${crc.value}.db"
    fs.copyFromLocalFile(Path(tempFilename), Path(storedFilename))

    if (andDelete) {
        fs.delete(Path(filename), false)
    }

    return storedFilename
}

/**
 * Load many VCF files into databases
 */
fun loadAllVcfRecordDbs(
    spark: SparkSession,
    aggHeader: AggVcfHeader,
    filterRanges: org.apache.spark.broadcast.Broadcast<GRangeIndex<GRange>>?,
    andDelete: Boolean = false,
    unfilteredRecordCount: LongAccumulator? = null,
    recordCount: LongAccumulator? = null,
    recordBytes: LongAccumulator? = null
): Dataset<Row> {
    val jsc = JavaSparkContext(spark.sparkContext)
    val filenamesRDD = jsc
        .parallelize(aggHeader.filenameCallsetId.toList())
        .repartition(jsc.defaultParallelism())
    val contigId = aggHeader.contigId
    return spark.createDataFrame(
        filenamesRDD
            .map(
                Function<Pair<String, Int>, Row> { (filename, callsetId) ->
                    val dbfn = loadVcfRecordDb(
                        contigId,
                        filename,
                        filterRanges = filterRanges?.value,
                        andDelete = andDelete,
                        unfilteredRecordCount = unfilteredRecordCount,
                        recordCount = recordCount,
                        recordBytes = recordBytes
                    )
                    RowFactory.create(callsetId, filename, dbfn)
                }
            ),
        StructType()
            .add("callsetId", DataTypes.IntegerType, false)
            .add("vcfFilename", DataTypes.StringType, false)
            .add("dbFilename", DataTypes.StringType, false)
    )
}

fun scanVcfRecordDb(
    contigId: Map<String, Short>,
    callsetId: Int,
    filename: String
): Sequence<VcfRecord> {
    return sequence {
        ExitStack().use { cleanup ->
            val localPath = cleanup.add(TempLocalFileCopy(filename)).localPath
            val dbc = cleanup.add(openGenomicSQLiteReadOnly(localPath))
            val stmt = cleanup.add(dbc.createStatement())
            val rs = cleanup.add(stmt.executeQuery("SELECT line from VcfRecord"))
            while (rs.next()) {
                val line = rs.getString("line")
                yield(parseVcfRecord(contigId, callsetId, line))
            }
        }
    }
}
