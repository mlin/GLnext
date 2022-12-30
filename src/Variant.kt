import java.io.File
import kotlin.math.min
import net.mlin.genomicsqlite.GenomicSQLite
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.sql.*
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.*
import org.jetbrains.kotlinx.spark.api.*

/**
 * Elementary variant
 */
data class Variant(val range: GRange, val ref: String, val alt: String) :
    Comparable<Variant>, java.io.Serializable {
    constructor(row: Row) :
        this(
            GRange(
                row.getAs<Short>("rid"),
                row.getAs<Int>("beg"),
                row.getAs<Int>("end")
            ),
            row.getAs<String>("ref"),
            row.getAs<String>("alt")
        )
    constructor(rs: java.sql.ResultSet) :
        this(
            GRange(
                rs.getShort("rid"),
                rs.getInt("beg") + 1,
                rs.getInt("end")
            ),
            rs.getString("ref"),
            rs.getString("alt")
        )
    override fun compareTo(other: Variant) = compareValuesBy(
        this,
        other,
        { it.range },
        { it.ref },
        { it.alt }
    )
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
 * Normalize variant by trimming reference padding
 * TODO: full, left-aligning normalization algo
 */
fun Variant.normalize(): Variant {
    var trimR = 0
    while (trimR < min(ref.length, alt.length) - 1 &&
        ref[ref.length - trimR - 1] == alt[alt.length - trimR - 1]
    ) {
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
 * Harvest variants from a VCF text line
 * onlyCalled: only include variants with at least one copy called in a sample GT
 */
fun discoverVariants(
    it: VcfRecord,
    filterRanges: org.apache.spark.broadcast.Broadcast<GRangeIndex<GRange>>?,
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
 * Harvest all distinct Variants from VcfRecordDbs
 * onlyCalled: only include variants with at least one copy called in a sample GT
 */
fun discoverAllVariants(
    aggHeader: AggVcfHeader,
    vcfRecordDbsDF: Dataset<Row>,
    filterRanges: org.apache.spark.broadcast.Broadcast<GRangeIndex<GRange>>?,
    onlyCalled: Boolean = false
): Dataset<Row> {
    val contigId = aggHeader.contigId
    return vcfRecordDbsDF
        .flatMap(
            FlatMapFunction<Row, Row> { row ->
                val callsetId = row.getAs<Int>("callsetId")
                val dbFilename = row.getAs<String>("dbFilename")
                val dbLocalFilename = row.getAs<String>("dbLocalFilename")
                sequence {
                    // Usually this task will run on the same executor that created the vcf records
                    // db, so we can open the existing local file. But if not then fetch the db
                    // from HDFS, where we copied it for this contingency.
                    ensureLocalCopy(dbFilename, dbLocalFilename)
                    scanVcfRecordDb(contigId, callsetId, dbLocalFilename)
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
    filterRanges: org.apache.spark.broadcast.Broadcast<GRangeIndex<GRange>>?,
    onlyCalled: Boolean = false
): Pair<Int, String> {
    val tempFile = File.createTempFile("Variant.", ".genomicsqlite")
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

    return variantId to tempFilename
}
