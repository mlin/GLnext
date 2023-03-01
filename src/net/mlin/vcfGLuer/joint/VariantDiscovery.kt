package net.mlin.vcfGLuer.joint
import java.io.File
import net.mlin.vcfGLuer.data.*
import net.mlin.vcfGLuer.util.*
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.util.LongAccumulator

/**
 * Harvest variants from a VCF record
 * filterRanges: only include variants contained within one of these ranges
 * onlyCalled: only include variants with at least one copy called in a sample GT
 */
fun discoverVariants(
    it: VcfRecord,
    filterRanges: org.apache.spark.broadcast.Broadcast<BedRanges>? = null,
    onlyCalled: Boolean = false
): List<Variant> {
    val vcfRecord = VcfRecordUnpacked(it)
    val variants = vcfRecord.altVariants.copyOf()
    if (!onlyCalled) {
        return variants.filterNotNull()
            .filter {
                    vt ->
                filterRanges?.let {
                    it.value!!.hasContaining(vt.range)
                } ?: true
            }
    }
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
    return variants.filterIndexed { i, _ -> copies[i] > 0 }
        .filterNotNull()
        .filter {
                vt ->
            filterRanges?.let {
                it.value!!.hasContaining(vt.range)
            } ?: true
        }
}

/**
 * Harvest all distinct Variants from the input VCF files
 */
fun discoverAllVariants(
    contigId: Map<String, Short>,
    vcfFilenamesDF: Dataset<Row>,
    filterRids: Set<Short>? = null,
    filterRanges: Broadcast<BedRanges>? = null,
    onlyCalled: Boolean = false,
    vcfRecordCount: LongAccumulator? = null,
    vcfRecordBytes: LongAccumulator? = null
): Dataset<Row> {
    return vcfFilenamesDF.flatMap(
        FlatMapFunction<Row, Row> { row ->
            sequence {
                scanVcfRecords(contigId, row.getAs<String>("vcfFilename")).use { records ->
                    records.forEach { rec ->
                        if (filterRids?.contains(rec.range.rid) ?: true) {
                            vcfRecordCount?.add(1L)
                            vcfRecordBytes?.add(rec.line.length.toLong() + 1L)
                            yieldAll(
                                discoverVariants(rec, filterRanges, onlyCalled)
                                    .map { it.toRow() }
                            )
                        }
                    }
                }
            }.iterator()
        },
        VariantRowEncoder()
    ).distinct() // TODO: accumulate instead of distinct() to collect summary stats
}

/**
 * Discover all variants & collect them into a GenomicSQLite file local to the driver.
 */
fun collectAllVariantsDb(
    contigId: Map<String, Short>,
    vcfPathsDF: Dataset<Row>,
    filterRids: Set<Short>? = null,
    filterRanges: Broadcast<BedRanges>? = null,
    onlyCalled: Boolean = false,
    vcfRecordCount: LongAccumulator? = null,
    vcfRecordBytes: LongAccumulator? = null
): Triple<Int, String, Long> {
    val tempFile = File.createTempFile("vcfGLuerVariants.", ".db")
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
        discoverAllVariants(
            contigId,
            vcfPathsDF,
            filterRids,
            filterRanges,
            onlyCalled,
            vcfRecordCount,
            vcfRecordBytes
        )
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
        dbc.commit()
    }

    val crc = fileCRC32C(tempFilename)
    val crcFile = File(tempFile.getParent(), "vcfGLuerVariants.$crc.db")
    tempFile.renameTo(crcFile)

    return Triple(variantId, crcFile.absolutePath, crc)
}
