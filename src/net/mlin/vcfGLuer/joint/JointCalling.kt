package net.mlin.vcfGLuer.joint
import java.io.Serializable
import kotlin.math.pow
import kotlin.text.StringBuilder
import net.mlin.vcfGLuer.data.*
import net.mlin.vcfGLuer.util.*
import org.apache.log4j.Logger
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.api.java.function.MapGroupsFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.LongAccumulator
import org.jetbrains.kotlinx.spark.api.*
import org.xerial.snappy.Snappy

enum class GT_OverlapMode {
    MISSING,
    SYMBOLIC,
    REF
}

data class JointGenotypeConfig(val overlapMode: GT_OverlapMode) : Serializable
data class JointConfig(
    val keepTrailingFields: Boolean,
    val gt: JointGenotypeConfig,
    val formatFields: List<JointFormatField>
) : Serializable

/**
 * From local databases of variants & VCF records, generate pVCF header and sorted RDD of the pVCF
 * lines
 */
fun jointCall(
    logger: Logger,
    cfg: JointConfig,
    spark: SparkSession,
    aggHeader: AggVcfHeader,
    variantCount: Int,
    variantsDbFilename: String,
    vcfFilenamesDF: Dataset<Row>,
    pvcfHeaderMetaLines: List<String> = emptyList(),
    pvcfRecordBytes: LongAccumulator? = null
): Pair<String, JavaRDD<String>> {
    // broadcast supporting data for joint calling; using JavaSparkContext to sidestep
    // kotlin-spark-api overrides
    val jsc = JavaSparkContext(spark.sparkContext)
    val aggHeaderB = jsc.broadcast(aggHeader)
    val fieldsGenB = jsc.broadcast(JointFieldsGenerator(cfg, aggHeader))

    // make big DataFrame of pVCF genotype entries: (variantId, sampleId, pvcfEntry)
    val entriesDF = vcfFilenamesDF.flatMap(
        FlatMapFunction<Row, Row> {
            generateJointCalls(
                cfg,
                aggHeaderB.value,
                fieldsGenB.value,
                variantsDbFilename,
                it.getAs<Int>("callsetId"),
                it.getAs<String>("vcfFilename")
            )
                .map { RowFactory.create(it.first, it.second, it.third) }
                .iterator()
        },
        RowEncoder.apply(
            StructType()
                .add("variantId", DataTypes.IntegerType, false)
                .add("sampleId", DataTypes.IntegerType, false)
                .add("entry", DataTypes.StringType, false)
        )
    )

    // group the genotype entries by variantId and generate pVCF text lines (Snappy-compressed)
    val pvcfLinesDF = entriesDF
        .groupByKey(MapFunction<Row, Int> { it.getAs<Int>(0) }, Encoders.INT())
        .mapGroups(
            MapGroupsFunction<Int, Row, Row> { variantId, variantEntries ->
                // look up this variant in the broadcast database file (using pooled resources...)
                val variant = ExitStack().use { cleanup ->
                    val variants = cleanup.add(
                        GenomicSQLiteReadOnlyPool.get(variantsDbFilename).getConnection()
                    )
                    val getVariant = cleanup.add(
                        variants.prepareStatement("SELECT * from Variant WHERE variantId = ?")
                    )
                    getVariant.setInt(1, variantId)
                    val rs = cleanup.add(getVariant.executeQuery())
                    check(rs.next())
                    Variant(rs)
                }
                // assemble the entries into the pVCF line
                val snappyLine = assembleJointLine(
                    cfg,
                    aggHeaderB.value,
                    fieldsGenB.value,
                    variant,
                    variantEntries
                )
                RowFactory.create(variantId, snappyLine)
            },
            RowEncoder.apply(
                StructType()
                    .add("variantId", DataTypes.IntegerType, false)
                    .add("snappyLine", DataTypes.BinaryType, false)
            )
            // persist before sorting: https://stackoverflow.com/a/56310076
        ).persist(org.apache.spark.storage.StorageLevel.DISK_ONLY())
    // Perform a count() to force pvcfLinesDF, ensuring it registers as an SQL query in the history
    // server before next dropping to RDD. This provides useful diagnostic info that would
    // otherwise go missing. The log message also provides a progress marker.
    val pvcfLineCount = pvcfLinesDF.count()
    logger.info("sorting & writing ${pvcfLineCount.pretty()} pVCF lines...")
    check(pvcfLineCount == variantCount.toLong())

    // sort pVCF rows by variantId and return the decompressed text of each line
    val pvcfHeader = jointVcfHeader(cfg, aggHeader, pvcfHeaderMetaLines, fieldsGenB.value)
    return pvcfHeader to pvcfLinesDF
        .toJavaRDD()
        .sortBy(
            Function<Row, Int> { it.getAs<Int>(0) },
            true,
            // Coalesce to fewer partitions now that the data size has been reduced, to output a
            // smaller number of reasonably-sized pVCF part files.
            // This is why we've dropped down to RDD -- Dataset.orderBy() doesn't provide direct
            // control of the output partitioning.
            jsc.defaultParallelism().toDouble().pow(3.0 / 4.0).toInt()
        ).map(
            Function<Row, String> { row ->
                val ans = String(Snappy.uncompress(row.getAs<ByteArray>(1)))
                pvcfRecordBytes?.let { it.add(ans.length + 1L) }
                ans
            }
        )
}

/**
 * Generate the pVCF genotype entries for one callset
 */
fun generateJointCalls(
    cfg: JointConfig,
    aggHeader: AggVcfHeader,
    fieldsGen: JointFieldsGenerator,
    variantsDbFilename: String,
    callsetId: Int,
    vcfFilename: String
): Sequence<Triple<Int, Int, String>> = // [(variantId, sampleId, pvcfEntry)]
    sequence {
        // variants sequence (read from broadcast database file)
        val variants = sequence {
            ExitStack().use { cleanup ->
                val variants = cleanup.add(
                    GenomicSQLiteReadOnlyPool.get(variantsDbFilename).getConnection()
                )
                val rs = cleanup.add(variants.createStatement()).executeQuery(
                    "SELECT * FROM Variant ORDER BY variantId"
                )
                while (rs.next()) {
                    val variantId = rs.getInt("variantId")
                    val variant = Variant(rs)
                    yield(variantId to variant)
                }
            }
        }
        scanVcfRecords(aggHeader.contigId, vcfFilename).use { records ->
            // collate the two sequences to generate GenotypingContexts with each variant and the
            // overlapping VCF records
            generateGenotypingContexts(variants, records).forEach { ctx ->
                aggHeader.callsetsDetails[callsetId].callsetSamples
                    .forEachIndexed { sampleIndex, sampleId ->
                        yield(
                            Triple(
                                ctx.variantId,
                                sampleId,
                                generateGenotypeAndFormatFields(
                                    cfg,
                                    fieldsGen,
                                    ctx,
                                    sampleIndex
                                )
                            )
                        )
                    }
            }
        }
    }

/**
 * Callset records assorted into reference bands, records containing the focal variant, and others
 */
class GenotypingContext(
    val variantId: Int,
    val variant: Variant,
    val callsetRecords: List<VcfRecordUnpacked>
) {
    val referenceBands: List<VcfRecordUnpacked>
    val variantRecords: List<VcfRecordUnpacked>
    val otherVariantRecords: List<VcfRecordUnpacked>

    init {
        callsetRecords.forEach { check(it.record.range.overlaps(variant.range)) }
        // reference bands have no (non-symbolic) ALT alleles
        var parts = callsetRecords.partition { it.altVariants.filterNotNull().isEmpty() }
        referenceBands = parts.first
        // partition variant records based on whether they include the focal variant
        parts = parts.second.partition { it.altVariants.contains(variant) }
        variantRecords = parts.first
        otherVariantRecords = parts.second
    }
}

/**
 * Collate variant & VCF record sequences (both range-sorted), producing the VariantCallingContext
 * for each variant with the overlapping VCF records (if any).
 *
 * The streaming algorithm assumes that the variants aren't too large and the VCF records aren't
 * too overlapping. These assumptions match up with small-variant gVCF inputs, of course.
 */
fun generateGenotypingContexts(
    variants: Sequence<Pair<Int, Variant>>, // (variantId, variant)
    recordsIter: Iterator<VcfRecord>
): Sequence<GenotypingContext> {
    // buffer of records whose ranges don't strictly precede (<<) the last-processed variant, nor
    // strictly follow (>>) any previously-processed variant
    val workingSet: MutableList<VcfRecordUnpacked> = mutableListOf()
    // an upcoming record that's >> the last-processed variant
    var record: VcfRecord? = null

    // (to verify sort order)
    var lastVariantRange: GRange? = null
    var lastRecordRange: GRange? = null

    // for each variant
    return variants.mapNotNull { (variantId, variant) ->
        val vr = variant.range
        lastVariantRange?.let { require(it <= vr) }
        lastVariantRange = vr

        // prune working set of records << variant
        workingSet.removeAll {
            it.record.range.rid < vr.rid ||
                (it.record.range.rid == vr.rid && it.record.range.end < vr.beg)
        }

        // consume records while not >> variant, adding to working set if not << variant
        if (record == null && recordsIter.hasNext()) {
            record = recordsIter.next()
        }
        while (record != null) {
            val rr = record!!.range
            lastRecordRange?.let {
                // VCF records are (rid,beg)-sorted but not necessarily (rid,beg,end)-sorted.
                // That's okay because we'll decide when to break based on beg, not end.
                require(it.rid < rr.rid || (it.rid == rr.rid && it.beg <= rr.beg))
            }
            lastRecordRange = rr

            if (rr.rid > vr.rid || (rr.rid == vr.rid && rr.beg > vr.end)) {
                // record >> variant; save for potential relevance to NEXT variant
                break
            } else if (rr.rid == vr.rid && rr.end >= vr.beg) {
                // record neither << nor >> variant; add to working set
                workingSet.add(VcfRecordUnpacked(record!!))
            } // else discard record << variant
            record = if (recordsIter.hasNext()) recordsIter.next() else null
        }

        // Working set may still hold records that overlapped a prior (lengthy) variant, but not
        // the focal one; they're to be excluded from the focal GenotypingContext, but retained in
        // the working set for the next one(s).
        //
        //   prior variant   |-----------------------------------|
        //   focal variant                  |------|
        // retained record                                     |----|
        val hits = workingSet.filter { it.record.range.overlaps(variant.range) }
        if (hits.isNotEmpty()) { GenotypingContext(variantId, variant, hits) } else { null }
    }
}

/**
 * Given GenotypingContext, generate the pVCF genotype entry and FORMAT fields for one sample
 */
fun generateGenotypeAndFormatFields(
    cfg: JointConfig,
    fieldsGen: JointFieldsGenerator,
    data: GenotypingContext,
    sampleIndex: Int
): String {
    if (data.variantRecords.isEmpty()) {
        return generateRefGenotypeAndFormatFields(cfg, fieldsGen, data, sampleIndex)
    }

    val record = if (data.variantRecords.size == 1) {
        data.variantRecords.first()
    } else {
        // choose one of variantRecords to work from
        // TODO: should we instead just no-call this unusual case?
        data.variantRecords.sortedByDescending {
            val altIdx = it.getAltIndex(data.variant)
            check(altIdx > 0)
            val gt = it.getDiploidGenotype(sampleIndex)
            // copy number
            var ans = listOf(gt.allele1, gt.allele2).filter { it == altIdx }.size
            // tie-breaker: prefer unphased
            ans = ans * 2
            if (!gt.phased) {
                ans++
            }
            ans
        }.first()
    }

    val altIdx = record.getAltIndex(data.variant)
    check(altIdx > 0)
    val gtIn = record.getDiploidGenotype(sampleIndex)

    // TODO: check otherVariantRecords; potentially override a REF call
    //     val altCopies = listOf(gtIn.allele1, gtIn.allele2).filter { it == altIdx }.size
    //     val otherOverlaps = countAltCopies(data.otherVariantRecords, sampleIndex)

    fun translate(inAllele: Int?): Int? = when (inAllele) {
        null -> null
        0 -> 0
        altIdx -> 1
        else -> genotypeOverlapSentinel(cfg.gt.overlapMode)
    }

    val gtOut = DiploidGenotype(
        translate(gtIn.allele1),
        translate(gtIn.allele2),
        gtIn.phased
    ).normalize()
    return gtOut.toString() + fieldsGen.generateFormatFields(data, sampleIndex, gtOut, record)
}

/**
 * id. for the case of no VCF records including the focal variant
 */
fun generateRefGenotypeAndFormatFields(
    cfg: JointConfig,
    fieldsGen: JointFieldsGenerator,
    data: GenotypingContext,
    sampleIndex: Int
): String {
    check(data.variantRecords.isEmpty())

    // count copies of other overlapping variants
    var otherOverlaps = countAltCopies(data.otherVariantRecords, sampleIndex)
    var refRanges = data.otherVariantRecords.map {
        val gt = it.getDiploidGenotype(sampleIndex)
        if (gt.allele1 != null && gt.allele2 != null) it.record.range else null
    }.filterNotNull()

    // check if overlapping records and reference bands completely covered the focal variant range
    refRanges += data.referenceBands.filter {
        val gt = it.getDiploidGenotype(sampleIndex)
        gt.allele1 == 0 && gt.allele2 == 0
    }.map { it.record.range }
    val refCoverage = data.variant.range.subtract(refRanges).isEmpty()
    val refCall = if (refCoverage) 0 else null

    val allele1 = if (otherOverlaps > 0) genotypeOverlapSentinel(cfg.gt.overlapMode) else refCall
    val allele2 = if (otherOverlaps > 1) genotypeOverlapSentinel(cfg.gt.overlapMode) else refCall

    val gtOut = DiploidGenotype(allele1, allele2, false).normalize()
    return gtOut.toString() + fieldsGen.generateFormatFields(data, sampleIndex, gtOut, null)
}

fun countAltCopies(records: List<VcfRecordUnpacked>, sampleIndex: Int): Int {
    // TODO: report ambiguity if there only mutually-non-overlapping unphased variants.
    // that is, distinguish whether or not the observed ALT copies might all be on one chromosome
    // or else must affect both.
    // This branches based on whether records have phase annotations or not.
    return records.flatMap {
        val gt = it.getDiploidGenotype(sampleIndex)
        listOf(gt.allele1, gt.allele2)
    }.filter { it != null && it > 0 }.size
}

fun genotypeOverlapSentinel(mode: GT_OverlapMode): Int? = when (mode) {
    GT_OverlapMode.MISSING -> null
    GT_OverlapMode.SYMBOLIC -> 2
    GT_OverlapMode.REF -> 0
}

/**
 * Given all the genotype entries for a specific variant, assemble the pVCF line as a compressed
 * buffer. The Snappy compression is just to reduce the amount of data Spark has to shuffle around
 * to sort the lines afterwards.
 */
fun assembleJointLine(
    cfg: JointConfig,
    aggHeader: AggVcfHeader,
    fieldsGen: JointFieldsGenerator,
    variant: Variant,
    entries: Iterator<Row>
): ByteArray {
    // prepare output TSV (array)
    val lineTsv = Array<String>(VcfColumn.FIRST_SAMPLE.ordinal + aggHeader.samples.size) { "." }
    lineTsv[VcfColumn.CHROM.ordinal] = aggHeader.contigs[variant.range.rid.toInt()] // CHROM
    lineTsv[VcfColumn.POS.ordinal] = variant.range.beg.toString() // POS
    // ID
    lineTsv[VcfColumn.REF.ordinal] = variant.ref // REF
    lineTsv[VcfColumn.ALT.ordinal] = variant.alt // ALT
    // QUAL
    lineTsv[VcfColumn.FILTER.ordinal] = "PASS" // FILTER
    // INFO
    lineTsv[VcfColumn.FORMAT.ordinal] =
        (listOf("GT") + cfg.formatFields.map { it.name })
            .joinToString(":") // FORMAT

    // fill entries into lineTsv
    entries.forEach {
        val sampleId = it.getAs<Int>("sampleId")
        val entry = it.getAs<String>("entry")
        val col = VcfColumn.FIRST_SAMPLE.ordinal + sampleId
        require(
            lineTsv[col] == ".",
            {
                "conflicting genotype entries @ ${aggHeader.samples[sampleId]}" +
                    " ${variant.str(aggHeader.contigs)}: ${lineTsv[col]} $entry"
            }
        )
        lineTsv[col] = entry
    }

    // TODO: compute INFO fields
    // lineTsv[VcfColumn.INFO.ordinal] = fieldsGen.generateInfoFields()

    // generate compressed line for pVCF sorting
    return Snappy.compress(lineTsv.joinToString("\t").toByteArray())
}

/**
 * Write pVCF header
 */
fun jointVcfHeader(
    cfg: JointConfig,
    aggHeader: AggVcfHeader,
    pvcfHeaderMetaLines: List<String>,
    fieldsGen: JointFieldsGenerator
): String {
    val ans = StringBuilder()
    ans.appendLine("##fileformat=VCFv4.3")

    pvcfHeaderMetaLines.forEach {
        ans.append("##")
        ans.appendLine(it)
    }

    // FILTER
    ans.appendLine("##FILTER=<ID=PASS,Description=\"All filters passed\">")

    // FORMAT/INFO
    ans.appendLine("##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">")
    fieldsGen.fieldHeaderLines().forEach {
        ans.append("##")
        ans.appendLine(it)
    }

    // contig
    aggHeader.contigs.forEach {
        ans.appendLine("##" + aggHeader.headerLines.get(VcfHeaderLineKind.CONTIG to it)!!.lineText)
    }

    // column headers
    ans.append(
        listOf(
            "#CHROM",
            "POS",
            "ID",
            "REF",
            "ALT",
            "QUAL",
            "FILTER",
            "INFO",
            "FORMAT"
        ).joinToString("\t")
    )
    aggHeader.samples.forEach {
        ans.append("\t" + it)
    }
    ans.appendLine("")

    return ans.toString()
}
