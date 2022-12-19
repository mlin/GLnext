
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
    variants: Array<Array<Variant>>,
    contigVcfRecordsDF: Dataset<Row>,
    pvcfHeaderMetaLines: List<String> = emptyList(),
    pvcfRecordCount: LongAccumulator? = null,
    pvcfRecordBytes: LongAccumulator? = null
): Pair<String, JavaRDD<String>> {
    // broadcast supporting data for joint calling; using JavaSparkContext to sidestep
    // kotlin-spark-api overrides
    val jsc = JavaSparkContext(spark.sparkContext)
    val variantsB = jsc.broadcast(variants)
    val aggHeaderB = jsc.broadcast(aggHeader)
    val fieldsGenB = jsc.broadcast(JointFieldsGenerator(cfg, aggHeader))

    // make big DataFrame of genotype calls: (variantId, sampleId, pvcfEntry)
    val entriesDF = contigVcfRecordsDF.flatMap(
        FlatMapFunction<Row, Row> {
            contigVcfRecordCalls(
                cfg,
                aggHeaderB.value,
                variantsB.value,
                fieldsGenB.value,
                ContigVcfRecords(it)
            ).iterator()
        },
        RowEncoder.apply(
            StructType()
                .add("variantId", DataTypes.LongType, false)
                .add("sampleId", DataTypes.IntegerType, false)
                .add("entry", DataTypes.StringType, false)
        )
    )

    // group genotypes by variantId and generate pVCF text lines (Snappy-compressed)
    val pvcfRowSchema = StructType()
        .add("variantId", DataTypes.LongType, false)
        .add("snappyLine", DataTypes.BinaryType, false)
    val pvcfRows = entriesDF
        .groupByKey(MapFunction<Row, Long> { it.getAs<Long>(0) }, Encoders.LONG())
        .mapGroups(
            MapGroupsFunction<Long, Row, Row> {
                    variantId, variantEntries ->
                // compute any INFO aggregates
                // lay out the entries into TSV
                pvcfRecordCount?.let { it.add(1L) }
                val rid = variantId shr 48
                val variantIndex = variantId - (rid shl 48)
                assembleJointLine(
                    cfg,
                    aggHeaderB.value,
                    fieldsGenB.value,
                    variantId,
                    variantsB.value[rid.toInt()][variantIndex.toInt()],
                    variantEntries
                )
            },
            RowEncoder.apply(pvcfRowSchema)
        ).cache() // cache() before sorting: https://stackoverflow.com/a/56310076
    // Perform a count() to force pvcfRows, ensuring it registers as an SQL query in the history
    // server before next dropping to RDD. This provides useful diagnostic info that would
    // otherwise go missing. The log message also provides a progress marker.
    val pvcfRowCount = pvcfRows.count()
    logger.info("sorting & writing $pvcfRowCount pVCF lines...")

    // sort pVCF rows by Variant and return the decompressed text of each line
    val pvcfHeader = jointVcfHeader(cfg, aggHeader, pvcfHeaderMetaLines, fieldsGenB.value)
    return pvcfHeader to pvcfRows
        .toJavaRDD()
        .sortBy(
            Function<Row, Long> { it.getAs<Long>(0) },
            true,
            // Coalesce to fewer partitions now that the data size has been greatly reduced,
            // compared to what we dealt with in the big join. Thus output a smaller number of
            // reasonably-sized pVCF part files.
            // This is why we've dropped down to RDD -- Dataset.orderBy() doesn't provide direct
            // control of the output partitioning.
            jsc.defaultParallelism().toDouble().pow(2.0 / 3.0).toInt()
        ).map(
            Function<Row, String> {
                    row ->
                val ans = String(Snappy.uncompress(row.getAs<ByteArray>(1)))
                pvcfRecordBytes?.let { it.add(ans.length + 1L) }
                ans
            }
        )
}

fun contigVcfRecordCalls(
    cfg: JointConfig,
    aggHeader: AggVcfHeader,
    variants: Array<Array<Variant>>,
    fieldsGen: JointFieldsGenerator,
    it: ContigVcfRecords
): Sequence<Row> {
    // extract & index the records
    val callsetId = it.callsetId
    val records = it.records(aggHeader.contigId).toList().toTypedArray()
    val rid = records[0].range.rid
    val treeBuilder = IntegerIntervalTree.Builder()
    records.forEach {
        check(it.range.rid == rid)
        treeBuilder.add(it.range.beg, it.range.end + 1)
    }
    check(treeBuilder.isSorted())
    val recordsTree = treeBuilder.build()

    // generate the genotype entry for each variant on the relevant contig
    return sequence {
        variants[rid.toInt()].forEachIndexed {
                variantIndex, variant ->
            // get overlapping records
            val ctx = VcfRecordsContext(
                aggHeader,
                variant,
                recordsTree.queryOverlap(variant.range.beg, variant.range.end + 1).map {
                    records[it.id]
                }
            )
            aggHeader.callsetsDetails[callsetId].callsetSamples.forEachIndexed {
                    sampleIndex, sampleId ->
                val entry = generateGenotypeAndFormatFields(cfg, fieldsGen, ctx, sampleIndex)
                val variantId = (rid.toLong() shl 48) + variantIndex.toLong()
                yield(RowFactory.create(variantId, sampleId, entry))
            }
        }
    }
}

fun assembleJointLine(
    cfg: JointConfig,
    aggHeader: AggVcfHeader,
    fieldsGen: JointFieldsGenerator,
    variantId: Long,
    variant: Variant,
    entries: Iterator<Row>
): Row {
    // prepare output TSV
    val recordTsv = initializeOutputTsv(cfg, aggHeader, variant)

    // fill entries into recordTsv
    entries.forEach {
        check(it.getAs<Long>("variantId") == variantId)
        val sampleId = it.getAs<Int>("sampleId")
        recordTsv[VcfColumn.FIRST_SAMPLE.ordinal + sampleId] = it.getAs<String>("entry")
    }

    // TODO: compute INFO fields
    // recordTsv[VcfColumn.INFO.ordinal] = fieldsGen.generateInfoFields()

    // generate compressed line for pVCF sorting
    val snappyLine = Snappy.compress(recordTsv.joinToString("\t").toByteArray())
    return RowFactory.create(variantId, snappyLine)
}

fun initializeOutputTsv(
    cfg: JointConfig,
    aggHeader: AggVcfHeader,
    variant: Variant
): Array<String> {
    val recordTsv = Array<String>(VcfColumn.FIRST_SAMPLE.ordinal + aggHeader.samples.size) { "." }
    recordTsv[VcfColumn.CHROM.ordinal] = aggHeader.contigs[variant.range.rid.toInt()] // CHROM
    recordTsv[VcfColumn.POS.ordinal] = variant.range.beg.toString() // POS
    // ID
    recordTsv[VcfColumn.REF.ordinal] = variant.ref // REF
    recordTsv[VcfColumn.ALT.ordinal] = variant.alt // ALT
    // QUAL
    recordTsv[VcfColumn.FILTER.ordinal] = "PASS" // FILTER
    // INFO
    recordTsv[VcfColumn.FORMAT.ordinal] =
        (listOf("GT") + cfg.formatFields.map { it.name })
            .joinToString(":") // FORMAT
    return recordTsv
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
        var parts = callsetRecords.map { VcfRecordUnpacked(it) }
            .partition { it.altVariants.filterNotNull().isEmpty() }
        referenceBands = parts.first
        // partition variant records based on whether they include the focal variant
        parts = parts.second.partition { it.altVariants.contains(variant) }
        variantRecords = parts.first
        otherVariantRecords = parts.second
    }
}
