package net.mlin.vcfGLuer.joint
import java.io.File
import net.mlin.vcfGLuer.data.*
import net.mlin.vcfGLuer.util.*
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.function.Function
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.*
import org.apache.spark.util.LongAccumulator

data class VariantStats(val copies: Int, val qual: Int?, val qual2: Int?) {
    constructor(row: Row) :
        this(
            row.getAs<Int>("copies"),
            row.getAs<Int?>("qual"),
            row.getAs<Int?>("qual2")
        )
    constructor(rs: java.sql.ResultSet) :
        this(
            rs.getInt("copies"),
            rs.getIntOrNull("qual"),
            rs.getIntOrNull("qual2")
        )
}
data class DiscoveredVariant(val variant: Variant, val stats: VariantStats) {
    constructor(row: Row) : this(Variant(row), VariantStats(row))
    constructor(rs: java.sql.ResultSet) : this(Variant(rs), VariantStats(rs))
}

/**
 * Harvest variants from a VCF record, yielding one DiscoveredVariant per ALT allele per sample
 * filterRanges: only include variants contained within one of these ranges
 */
fun discoverVariants(
    it: VcfRecord,
    filterRanges: Broadcast<BedRanges>? = null
): Sequence<DiscoveredVariant> {
    return sequence {
        val vcfRecord = VcfRecordUnpacked(it)
        for (sampleIndex in 0 until vcfRecord.sampleCount) {
            val qualities = vcfRecord.getSampleAltQualities(sampleIndex)
            val copies = vcfRecord.altVariants.map { 0 }.toTypedArray()
            val gt = vcfRecord.getDiploidGenotype(sampleIndex)
            if (gt.allele1 != null && gt.allele1 > 0) {
                copies[gt.allele1 - 1]++
            }
            if (gt.allele2 != null && gt.allele2 > 0) {
                copies[gt.allele2 - 1]++
            }

            vcfRecord.altVariants.forEachIndexed { i, vt ->
                if (vt != null &&
                    (filterRanges?.let { it.value!!.hasContaining(vt.range) } ?: true)
                ) {
                    yield(
                        DiscoveredVariant(vt, VariantStats(copies[i], qualities[i], null))
                    )
                }
            }
        }
    }
}

/**
 * Harvest all Variants from the input VCF files, along with total copies, max quality score, and
 * second-ranked quality score.
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
    val variants = vcfFilenamesDF.flatMap(
        FlatMapFunction<Row, Row> { row ->
            sequence {
                scanVcfRecords(contigId, row.getAs<String>("vcfFilename")).use { records ->
                    records.forEach { rec ->
                        if (filterRids?.contains(rec.range.rid) ?: true) {
                            vcfRecordCount?.add(1L)
                            vcfRecordBytes?.add(rec.line.length.toLong() + 1L)
                            yieldAll(
                                discoverVariants(rec, filterRanges)
                                    .map {
                                        check(it.stats.qual2 == null)
                                        it.variant.toRow(
                                            copies = it.stats.copies,
                                            qual = it.stats.qual
                                        )
                                    }
                            )
                        }
                    }
                }
            }.iterator()
        },
        VariantRowEncoder()
    ).groupBy("rid", "beg", "end", "ref", "alt")
        // aggregate each variant's copy number and top two quality scores
        .agg(
            sum("copies").`as`("copies"),
            max("qual").`as`("qual"),
            udaf(
                NthLargestInt(2),
                org.apache.spark.sql.Encoders.INT()
            ).apply(col("qual")).`as`("qual2")
        )

    if (onlyCalled) {
        return variants.filter("copies > 0")
    }
    return variants
}

/**
 * Discover all variants & collect them into a GenomicSQLite file local to the driver.
 */
fun collectAllVariantsDb(
    contigId: Map<String, Short>,
    vcfPathsDF: Dataset<Row>,
    splitRanges: BedRanges,
    filterRids: Set<Short>? = null,
    filterRanges: Broadcast<BedRanges>? = null,
    onlyCalled: Boolean = false,
    vcfRecordCount: LongAccumulator? = null,
    vcfRecordBytes: LongAccumulator? = null
): Pair<Int, String> {
    val tempFile = File.createTempFile("vcfGLuerVariants.", ".db")
    val tempFilename = tempFile.absolutePath
    tempFile.delete()

    var variantId = 0

    ExitStack().use { cleanup ->
        val dbc = cleanup.add(createGenomicSQLiteForBulkLoad(tempFilename, threads = 8))
        val stmt = cleanup.add(dbc.createStatement())
        stmt.executeUpdate(
            """
            CREATE TABLE Variant(
                variantId INTEGER PRIMARY KEY,
                rid INTEGER NOT NULL,
                beg INTEGER NOT NULL,
                end INTEGER NOT NULL,
                ref TEXT NOT NULL,
                alt TEXT NOT NULL,
                copies INTEGER NOT NULL,
                qual INTEGER,
                qual2 INTEGER,
                splitId INTEGER NOT NULL,
                frameno INTEGER NOT NULL
            )
            """
        )
        val insert = cleanup.add(
            dbc.prepareStatement("INSERT INTO Variant VALUES(?,?,?,?,?,?,?,?,?,?,?)")
        )
        val allVariantsDF = discoverAllVariants(
            contigId,
            vcfPathsDF,
            filterRids,
            filterRanges,
            onlyCalled,
            vcfRecordCount,
            vcfRecordBytes
        ).coalesce(256).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK())
        allVariantsDF.count()
        // Drop to RDD<Variant>, sort, and cache before processing on the driver.
        // Using RDD.sortBy() allows us to control the partitioning, which is very important
        // to reduce the overhead in serially processing each partition with toLocalIterator().
        //
        // Since we intend to collect the variants on the driver anyway, we can assume 16
        // partitions should avoid any of them being too large for any executor.
        //
        // Ideally we'd just use Dataset<Row>.orderBy() and rely on AQE to make the partitioning
        // reasonable, but it seems that caching can trip up AQE:
        //   https://community.databricks.com/s/question/0D53f00001tDGYHCA4/spark-3-aqe-and-cache
        // and the toLocalIterator() docs say it should be used on cached data.
        //
        // (Similarly, above we explicitly coalesced the DF before caching+sorting.)
        val sortedVariantsRDD = allVariantsDF.toJavaRDD()
            .map(Function<Row, DiscoveredVariant> { row -> DiscoveredVariant(row) })
            .sortBy(Function<DiscoveredVariant, Variant> { it.variant }, true, 16)
            .persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_SER())
        sortedVariantsRDD.count() // force cache materialization before toLocalIterator()
        check(sortedVariantsRDD.getNumPartitions() <= 16)
        allVariantsDF.unpersist()

        // for each variant (locally on the driver)
        var lastSplitId = -1
        var frameno = 0
        sortedVariantsRDD
            .toLocalIterator()
            .forEach { dv ->
                val vt = dv.variant
                // find the output file splitting range this variant belongs to
                val splitId = splitRanges.queryOverlapping(
                    GRange(vt.range.rid, vt.range.beg, vt.range.beg)
                ).let {
                    require(
                        it.size == 1,
                        { "--split-bed regions overlap or don't cover @ ${vt.range}" }
                    )
                    it.first().id
                }
                // start a new frame (for sparse genotype encoding) every 128 variants or whenever
                // we advance to a new split range
                if ((variantId + 1) % 128 == 0 || splitId != lastSplitId) {
                    check(splitId >= lastSplitId)
                    frameno += 1
                }
                // insert into GenomicSQLite
                insert.setInt(1, variantId)
                insert.setInt(2, vt.range.rid.toInt())
                insert.setInt(3, vt.range.beg - 1)
                insert.setInt(4, vt.range.end)
                insert.setString(5, vt.ref)
                insert.setString(6, vt.alt)
                insert.setInt(7, dv.stats.copies)
                if (dv.stats.qual != null) {
                    insert.setInt(8, dv.stats.qual)
                } else {
                    insert.setNull(8, java.sql.Types.INTEGER)
                }
                if (dv.stats.qual2 != null) {
                    check(dv.stats.qual2 <= (dv.stats.qual ?: Int.MAX_VALUE))
                    insert.setInt(9, dv.stats.qual2)
                } else {
                    insert.setNull(9, java.sql.Types.INTEGER)
                }
                insert.setInt(10, splitId)
                insert.setInt(11, frameno)
                insert.executeUpdate()
                variantId += 1
                lastSplitId = splitId
            }
        // index frameno & commit
        stmt.executeUpdate("CREATE INDEX VariantFrameno ON Variant(frameno,variantId)")
        dbc.commit()
        sortedVariantsRDD.unpersist()
    }

    val crc = fileCRC32C(tempFilename)
    val crcFile = File(tempFile.getParent(), "vcfGLuerVariants.$crc.db")
    tempFile.renameTo(crcFile)

    return variantId to crcFile.absolutePath
}
