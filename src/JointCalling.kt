
import java.io.Serializable
import kotlin.math.pow
import net.mlin.iitj.IntegerIntervalTree
import org.apache.log4j.Logger
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.api.java.function.MapGroupsFunction
import org.apache.spark.sql.*
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
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
 * From variants & contigVcfRecordsDF, generate pVCF header and sorted RDD of the pVCF lines
 *     (variantId, sampleId, pvcfEntry)
 */
fun jointCall(
    logger: Logger,
    cfg: JointConfig,
    spark: SparkSession,
    aggHeader: AggVcfHeader,
    variants: GRangeIndex<Variant>,
    contigVcfRecordsDF: Dataset<Row>,
    pvcfHeaderMetaLines: List<String> = emptyList(),
    pvcfRecordCount: LongAccumulator? = null,
    pvcfRecordBytes: LongAccumulator? = null
): Pair<String, JavaRDD<String>> {
    // broadcast supporting data for joint calling; using JavaSparkContext to sidestep
    // kotlin-spark-api overrides
    val jsc = JavaSparkContext(spark.sparkContext)
    val aggHeaderB = jsc.broadcast(aggHeader)
    val fieldsGenB = jsc.broadcast(JointFieldsGenerator(cfg, aggHeader))
    val variantsB = jsc.broadcast(variants)

    // make big DataFrame of genotype entries: (variantId, sampleId, pvcfEntry)
    val entriesDF = contigVcfRecordsDF.flatMap(
        FlatMapFunction<Row, Row> {
            generateContigVcfRecordsCalls(
                cfg,
                aggHeaderB.value,
                fieldsGenB.value,
                variantsB.value,
                ContigVcfRecords(it)
            )
                .map { RowFactory.create(it.first, it.second, it.third) }
                .iterator()
        },
        RowEncoder.apply(
            StructType()
                .add("variantId", DataTypes.LongType, false)
                .add("sampleId", DataTypes.IntegerType, false)
                .add("entry", DataTypes.StringType, false)
        )
    )

    // group the genotype entries by variantId and generate pVCF text lines (Snappy-compressed)
    val pvcfLinesDF = entriesDF
        .groupByKey(MapFunction<Row, Long> { it.getAs<Long>(0) }, Encoders.LONG())
        .mapGroups(
            MapGroupsFunction<Long, Row, Row> { variantId, variantEntries ->
                pvcfRecordCount?.let { it.add(1L) }
                RowFactory.create(
                    variantId,
                    generateJointLine(
                        cfg,
                        aggHeaderB.value,
                        fieldsGenB.value,
                        variantsB.value.item(variantId),
                        variantEntries
                    )
                )
            },
            RowEncoder.apply(
                StructType()
                    .add("variantId", DataTypes.LongType, false)
                    .add("snappyLine", DataTypes.BinaryType, false)
            )
        ).cache() // cache() before sorting: https://stackoverflow.com/a/56310076
    // Perform a count() to force pvcfRows, ensuring it registers as an SQL query in the history
    // server before next dropping to RDD. This provides useful diagnostic info that would
    // otherwise go missing. The log message also provides a progress marker.
    val pvcfLineCount = pvcfLinesDF.count()
    logger.info("sorting & writing ${pvcfLineCount.pretty()} pVCF lines...")

    // sort pVCF rows by variantId and return the decompressed text of each line
    val pvcfHeader = jointVcfHeader(cfg, aggHeader, pvcfHeaderMetaLines, fieldsGenB.value)
    return pvcfHeader to pvcfLinesDF
        .toJavaRDD()
        .sortBy(
            Function<Row, Long> { it.getAs<Long>(0) },
            true,
            // Coalesce to fewer partitions now that the data size has been reduced, to output a
            // smaller number of reasonably-sized pVCF part files.
            // This is why we've dropped down to RDD -- Dataset.orderBy() doesn't provide direct
            // control of the output partitioning.
            jsc.defaultParallelism().toDouble().pow(2.0 / 3.0).toInt()
        ).map(
            Function<Row, String> { row ->
                val ans = String(Snappy.uncompress(row.getAs<ByteArray>(1)))
                pvcfRecordBytes?.let { it.add(ans.length + 1L) }
                ans
            }
        )
}

fun generateContigVcfRecordsCalls(
    cfg: JointConfig,
    aggHeader: AggVcfHeader,
    fieldsGen: JointFieldsGenerator,
    variants: GRangeIndex<Variant>,
    it: ContigVcfRecords
): Sequence<Triple<Long, Int, String>> {
    // extract the individual records & index them for interval query
    val callsetId = it.callsetId
    val records = it.records(aggHeader.contigId).toList().toTypedArray()
    val rid = records[0].range.rid
    val treeBuilder = IntegerIntervalTree.Builder()
    records.forEach {
        check(it.range.rid == rid)
        treeBuilder.add(it.range.beg - 1, it.range.end)
    }
    check(treeBuilder.isSorted())
    val recordsTree = treeBuilder.build()

    // generate the genotype entry for each variant on the relevant contig
    return sequence {
        variants.containedBy(it.range).forEach { variantId ->
            val variant = variants.item(variantId)

            // get the callet VCF records overlapping the variant
            val ctx = VcfRecordsContext(
                aggHeader,
                variant,
                recordsTree.queryOverlap(variant.range.beg - 1, variant.range.end).map {
                    records[it.id]
                }
            )

            // generate genotype entries
            aggHeader.callsetsDetails[callsetId].callsetSamples.forEachIndexed {
                    sampleIndex, sampleId ->
                yield(
                    Triple(
                        variantId,
                        sampleId,
                        generateGenotypeAndFormatFields(cfg, fieldsGen, ctx, sampleIndex)
                    )
                )
            }
        }
    }
}

fun generateJointLine(
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
            lineTsv[col] == "." || lineTsv[col] == entry,
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

fun generateGenotypeAndFormatFields(
    cfg: JointConfig,
    fieldsGen: JointFieldsGenerator,
    data: VcfRecordsContext,
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

fun generateRefGenotypeAndFormatFields(
    cfg: JointConfig,
    fieldsGen: JointFieldsGenerator,
    data: VcfRecordsContext,
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
 * Callset records assorted into reference bands, records containing the desired variant,
 * and other variants
 */
class VcfRecordsContext(
    val aggHeader: AggVcfHeader,
    val variant: Variant,
    val callsetRecords: List<VcfRecord>
) {
    val referenceBands: List<VcfRecordUnpacked>
    val variantRecords: List<VcfRecordUnpacked>
    val otherVariantRecords: List<VcfRecordUnpacked>

    init {
        // reference bands have no (non-symbolic) ALT alleles
        var parts = callsetRecords.map {
            check(it.range.overlaps(variant.range))
            VcfRecordUnpacked(it)
        }.partition { it.altVariants.filterNotNull().isEmpty() }
        referenceBands = parts.first
        // partition variant records based on whether they include the focal variant
        parts = parts.second.partition { it.altVariants.contains(variant) }
        variantRecords = parts.first
        otherVariantRecords = parts.second
    }
}
