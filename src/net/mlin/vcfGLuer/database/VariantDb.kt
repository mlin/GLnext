package net.mlin.vcfGLuer.database
import java.io.File
import net.mlin.genomicsqlite.GenomicSQLite
import net.mlin.vcfGLuer.datamodel.*
import net.mlin.vcfGLuer.util.*
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

/**
 * Harvest all distinct Variants from VcfRecordDbs
 * onlyCalled: only include variants with at least one copy called in a sample GT
 */
fun discoverAllVariants(
    aggHeader: AggVcfHeader,
    vcfRecordDbsDF: Dataset<Row>,
    filterRanges: Broadcast<BedRanges>?,
    onlyCalled: Boolean = false
): Dataset<Row> {
    val contigId = aggHeader.contigId
    return vcfRecordDbsDF
        .flatMap(
            FlatMapFunction<Row, Row> { row ->
                val dbPath = Path(row.getAs<String>("dbPath"))
                val dbLocalFilename = row.getAs<String>("dbLocalFilename")
                sequence {
                    // Usually this task will run on the same executor that created the vcf records
                    // db, so we can open the existing local file. But if not then fetch the db
                    // from HDFS, where we copied it for this contingency.
                    ensureLocalCopy(dbPath, dbLocalFilename)
                    scanVcfRecordDb(contigId, dbLocalFilename)
                        .forEach { rec ->
                            yieldAll(
                                discoverVariants(rec, filterRanges, onlyCalled)
                                    .map { it.toRow() }
                            )
                        }
                }.iterator()
            },
            VariantRowEncoder()
        ).distinct() // TODO: accumulate instead of distinct() to collect summary stats
}

/**
 * Discover all variants & collect them into a database file local to the driver.
 */
fun collectAllVariantsDb(
    aggHeader: AggVcfHeader,
    vcfRecordDbsDF: Dataset<Row>,
    filterRanges: Broadcast<BedRanges>?,
    onlyCalled: Boolean = false
): Pair<Int, String> {
    val tempFile = File.createTempFile("Variant.", ".db")
    val tempFilename = tempFile.absolutePath
    tempFile.delete()

    var variantId = 0

    ExitStack().use { cleanup ->
        val dbc = cleanup.add(createGenomicSQLiteForBulkLoad(tempFilename, threads = 8))
        val adhoc = cleanup.add(dbc.createStatement())
        adhoc.executeUpdate(
            """
            CREATE TABLE Variant(
                variantId INTEGER PRIMARY KEY,
                rid INTEGER NOT NULL,
                beg INTEGER NOT NULL,
                end INTEGER NOT NULL,
                ref TEXT NOT NULL,
                alt TEXT NOT NULL
            )
            """
        )
        val insert = cleanup.add(dbc.prepareStatement("INSERT INTO Variant VALUES(?,?,?,?,?,?)"))
        discoverAllVariants(aggHeader, vcfRecordDbsDF, filterRanges, onlyCalled)
            .orderBy("rid", "beg", "end", "ref", "alt")
            .toLocalIterator()
            .forEach { row ->
                insert.setInt(1, variantId)
                insert.setInt(2, row.getAs<Int>("rid"))
                insert.setInt(3, row.getAs<Int>("beg") - 1)
                insert.setInt(4, row.getAs<Int>("end"))
                insert.setString(5, row.getAs<String>("ref"))
                insert.setString(6, row.getAs<String>("alt"))
                insert.executeUpdate()
                variantId += 1
            }
        adhoc.executeUpdate(
            GenomicSQLite.createGenomicRangeIndexSQL(
                dbc,
                "Variant",
                "rid",
                "beg",
                "end"
            )
        )
        dbc.commit()
    }

    val crc = fileCRC32C(tempFilename)
    val crcFile = File(tempFile.getParent(), "Variant.$crc.db")
    tempFile.renameTo(crcFile)

    return variantId to crcFile.absolutePath
}
