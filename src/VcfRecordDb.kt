import java.io.File
import java.sql.*
import net.mlin.genomicsqlite.GenomicSQLite
import org.apache.hadoop.fs.FileSystem
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
    filterRanges: BedRanges?,
    andDelete: Boolean = false,
    unfilteredRecordCount: LongAccumulator? = null,
    recordCount: LongAccumulator? = null,
    recordBytes: LongAccumulator? = null,
    fs: FileSystem? = null
): String {
    val fs2 = fs ?: getFileSystem(filename)
    val tempFile = File.createTempFile(Path(filename).getName() + ".", ".db")!!
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
        fileReaderDetectGz(filename, fs2).useLines {
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

    val crc = fileCRC32C(tempFilename)
    val crcFile = File(
        tempFile.getParent(),
        Path(filename).getName() + ".$crc.db"
    )
    tempFile.renameTo(crcFile)

    if (andDelete) {
        fs2.delete(Path(filename), false)
    }

    return crcFile.absolutePath
}

/**
 * Load many VCF files into databases (and copy them back to HDFS)
 */
fun loadAllVcfRecordDbs(
    spark: SparkSession,
    aggHeader: AggVcfHeader,
    filterRanges: org.apache.spark.broadcast.Broadcast<BedRanges>?,
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
                    val fs = getFileSystem(filename)
                    val localDb = loadVcfRecordDb(
                        contigId,
                        filename,
                        filterRanges = filterRanges?.value,
                        andDelete = andDelete,
                        unfilteredRecordCount = unfilteredRecordCount,
                        recordCount = recordCount,
                        recordBytes = recordBytes,
                        fs = fs
                    )
                    // copy db file to HDFS, return both HDFS and local paths
                    val storedPath = Path(Path(filename).getParent(), Path(localDb).getName())
                    fs.copyFromLocalFile(Path(localDb), storedPath)
                    RowFactory.create(callsetId, filename, storedPath.toString(), localDb)
                }
            ),
        StructType()
            .add("callsetId", DataTypes.IntegerType, false)
            .add("vcfFilename", DataTypes.StringType, false)
            .add("dbFilename", DataTypes.StringType, false)
            .add("dbLocalFilename", DataTypes.StringType, false)
    )
}

fun scanVcfRecordDb(
    contigId: Map<String, Short>,
    callsetId: Int,
    filename: String
): Sequence<VcfRecord> {
    return sequence {
        ExitStack().use { cleanup ->
            val dbc = cleanup.add(openGenomicSQLiteReadOnly(filename))
            val stmt = cleanup.add(dbc.createStatement())
            val rs = cleanup.add(stmt.executeQuery("SELECT line from VcfRecord"))
            while (rs.next()) {
                val line = rs.getString("line")
                yield(parseVcfRecord(contigId, callsetId, line))
            }
        }
    }
}
