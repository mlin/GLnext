
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.api.java.function.MapGroupsFunction
import org.apache.spark.sql.*
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
import org.apache.spark.util.LongAccumulator
import org.jetbrains.kotlinx.spark.api.*
import org.xerial.snappy.Snappy
import java.io.Serializable

enum class GT_OverlapMode {
    MISSING,
    SYMBOLIC,
    REF
}

data class JointGenotypeConfig(val overlapMode: GT_OverlapMode) : Serializable
data class JointConfig(val gt: JointGenotypeConfig, val formatFields: List<JointFormatField>) : Serializable

/**
 * Joint-call variantsDF & vcfRecordsDF into sorted pVCF lines
 */
fun jointCall(
    cfg: JointConfig,
    spark: org.apache.spark.sql.SparkSession,
    aggHeader: AggVcfHeader,
    variantsDF: Dataset<Row>,
    vcfRecordsDF: Dataset<Row>,
    binSize: Int,
    pvcfHeaderMetaLines: List<String> = emptyList(),
    pvcfRecordCount: LongAccumulator? = null,
    pvcfRecordBytes: LongAccumulator? = null
): Pair<String, Dataset<String>> {
    val vcfRecordsCompressed = vcfRecordsDF.columns().contains("snappyLine")
    val aggHeaderB = spark.broadcast(aggHeader)
    // FORMAT/INFO field helpers
    val fieldsGen = JointFieldsGenerator(cfg, aggHeader)
    val fieldsGenB = spark.broadcast(fieldsGen)
    // joint-call each variant into a snappy-compressed pVCF line with GRange columns
    val pvcfToSort = joinVariantsAndVcfRecords(variantsDF, vcfRecordsDF, vcfRecordsCompressed, binSize).mapGroups(
        object : MapGroupsFunction<Row, Row, Row> {
            override fun call(variantRow: Row, callsetsData: Iterator<Row>): Row {
                val ans = jointCallVariant(cfg, aggHeaderB.value, fieldsGenB.value, variantRow, callsetsData, vcfRecordsCompressed)
                pvcfRecordCount?.let { it.add(1L) }
                return ans
            }
        },
        RowEncoder.apply(
            StructType()
                .add("rid", DataTypes.ShortType, false)
                .add("beg", DataTypes.IntegerType, false)
                .add("end", DataTypes.IntegerType, false)
                .add("alt", DataTypes.StringType, false)
                .add("snappyRecord", DataTypes.BinaryType, false)
        )
    )
    // formulate header
    val pvcfHeader = jointVcfHeader(cfg, aggHeader, pvcfHeaderMetaLines, fieldsGen)
    // sort pVCF lines by GRange & decompress
    return pvcfHeader to pvcfToSort
        .orderBy("rid", "beg", "end", "alt")
        .map {
            val ans = String(Snappy.uncompress(it.getAs<ByteArray>("snappyRecord")))
            pvcfRecordBytes?.let { it.add(ans.length + 1L) }
            ans
        }
}

/**
 * Join discovered variants with overlapping VCF records from each callset
 *
 * Keys of the returned groups are Variant rows
 * Values in the returned groups are:
 *  |-- callsetId: integer (nullable = true)
 *  |-- callsetLines: array (nullable = false)
 *  |    |-- element: binary (containsNull = false)
 * nb: the callsetLines are unsorted
 */
fun joinVariantsAndVcfRecords(variantsDF: Dataset<Row>, vcfRecordsDF: Dataset<Row>, vcfRecordsCompressed: Boolean, binSize: Int): KeyValueGroupedDataset<Row, Row> {
    // explode the variants & records across the GRange bins they touch
    val binnedVariants = variantsDF.selectExpr("explode(GRangeBins(rid,beg,end,$binSize)) as bin", "*").alias("var")
    val binnedRecords = vcfRecordsDF.selectExpr("explode(GRangeBins(rid,beg,end,$binSize)) as bin", "*").alias("vcf")

    val joinDFpre =
        binnedVariants
            // join variants & records by bin
            .join(binnedRecords, col("var.bin") eq col("vcf.bin"), "left")
            // filter joined bins by precise GRange overlap
            .filter((col("var.beg") leq col("vcf.end")) and (col("var.end") geq col("vcf.beg")))
            // group overlappers by (variant, callsetId)
            .groupBy("var.rid", "var.beg", "var.end", "var.ref", "var.alt", "vcf.callsetId")

    // from each such group, collect -distinct- VCF records (thus removing any binning-derived duplication)
    val joinDF = if (vcfRecordsCompressed) {
        joinDFpre.agg(collect_set("vcf.snappyLine").alias("callsetSnappyLines"))
    } else {
        joinDFpre.agg(collect_set("vcf.line").alias("callsetLines"))
    }

    // finally group the (variant, callsetId, callsetSnappyLines) items by variant
    // using groupByKey instead of (relational) groupBy so that we can mapGroups for joint calling.
    // TODO: sortedGroupByKey to eliminate final sorting shuffle
    //       joinDF.orderBy("rid","beg","end","alt").mapPartitions(grouperFn)
    //       check if this would guarantee: IF two items have the same orderBy key, THEN they'll go into the same partition
    //       https://github.com/apache/spark/blob/20ffbf7b308c3dc90a49dbdcb8d7b972eeb53bc4/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/physical/partitioning.scala
    //       OrderedDistribution+RangePartitioning may satisfy this. Could use accumulator to cheaply verify that the correct number of groups are collected.
    return joinDF
        .groupByKey(
            object : MapFunction<Row, Row> {
                override fun call(row: Row): Row {
                    val range = GRange(row.getAs<Short>("rid"), row.getAs<Int>("beg"), row.getAs<Int>("end"))
                    return Variant(range, row.getAs<String>("ref"), row.getAs<String>("alt")).toRow()
                }
            },
            VariantRowEncoder()
        )
}

/**
 * Generate pVCF record (with GRange columns) for one variant given the data group
 */
fun jointCallVariant(cfg: JointConfig, aggHeader: AggVcfHeader, fieldsGen: JointFieldsGenerator, variantRow: Row, callsetsData: Iterator<Row>, vcfRecordsCompressed: Boolean): Row {
    // extract variant
    val variantRange = GRange(variantRow.getAs<Short>("rid"), variantRow.getAs<Int>("beg"), variantRow.getAs<Int>("end"))
    val variant = Variant(variantRange, variantRow.getAs<String>("ref"), variantRow.getAs<String>("alt"))

    // prepare output record
    val recordTsv = initializeOutputTsv(cfg, aggHeader, variant)

    // for each callset
    for (callsetRow in callsetsData) {
        // parse the input VCF records overlapping the variant
        val unpackedRecords = UnpackedVcfRecords(aggHeader, variant, callsetRow, vcfRecordsCompressed)

        // generate genotype & FORMAT fields for each sample in the callset
        aggHeader.callsetsDetails[unpackedRecords.callsetId].callsetSamples.forEachIndexed {
            inSampleIdx, outSampleIdx ->
            if (outSampleIdx >= 0) {
                check(recordTsv[VcfColumn.FIRST_SAMPLE.ordinal + outSampleIdx] == ".", {
                    "duplicate genotype entries ${variant.str(aggHeader.contigs)} ${aggHeader.samples[outSampleIdx]}"
                })
                // fill in the output record
                recordTsv[VcfColumn.FIRST_SAMPLE.ordinal + outSampleIdx] = generateGenotypeAndFormatFields(cfg, fieldsGen, unpackedRecords, inSampleIdx)
            }
        }
    }

    // populate INFO fields
    recordTsv[VcfColumn.INFO.ordinal] = fieldsGen.generateInfoFields()

    // generate compressed line for pVCF sorting
    val snappyRecord = Snappy.compress(recordTsv.joinToString("\t").toByteArray())
    return RowFactory.create(variant.range.rid, variant.range.beg, variant.range.end, variant.alt, snappyRecord)
}

fun initializeOutputTsv(cfg: JointConfig, aggHeader: AggVcfHeader, variant: Variant): Array<String> {
    val recordTsv = Array<String>(VcfColumn.FIRST_SAMPLE.ordinal + aggHeader.samples.size) { "." }
    recordTsv[VcfColumn.CHROM.ordinal] = aggHeader.contigs[variant.range.rid.toInt()] // CHROM
    recordTsv[VcfColumn.POS.ordinal] = variant.range.beg.toString() // POS
    // ID
    recordTsv[VcfColumn.REF.ordinal] = variant.ref // REF
    recordTsv[VcfColumn.ALT.ordinal] = variant.alt // ALT
    // QUAL
    recordTsv[VcfColumn.FILTER.ordinal] = "PASS" // FILTER
    // INFO
    recordTsv[VcfColumn.FORMAT.ordinal] = (listOf("GT") + cfg.formatFields.map { it.name }).joinToString(":") // FORMAT
    return recordTsv
}

fun generateGenotypeAndFormatFields(cfg: JointConfig, fieldsGen: JointFieldsGenerator, data: UnpackedVcfRecords, sampleIndex: Int): String {
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

    val gtOut = DiploidGenotype(translate(gtIn.allele1), translate(gtIn.allele2), gtIn.phased).normalize()
    return gtOut.toString() + fieldsGen.generateFormatFields(data, sampleIndex, gtOut, record)
}

fun generateRefGenotypeAndFormatFields(cfg: JointConfig, fieldsGen: JointFieldsGenerator, data: UnpackedVcfRecords, sampleIndex: Int): String {
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
    val refCall = if (data.variant.range.subtract(refRanges).isEmpty()) 0 else null

    val allele1 = if (otherOverlaps > 0) genotypeOverlapSentinel(cfg.gt.overlapMode) else refCall
    val allele2 = if (otherOverlaps > 1) genotypeOverlapSentinel(cfg.gt.overlapMode) else refCall

    if (allele1 == null && allele2 == null) {
        return "."
    }
    val gtOut = DiploidGenotype(allele1, allele2, false).normalize()
    return gtOut.toString() + fieldsGen.generateFormatFields(data, sampleIndex, gtOut, null)
}

fun countAltCopies(records: List<UnpackedVcfRecord>, sampleIndex: Int): Int {
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
 * Helpers to assort callset records into reference bands, records containing the desired variant;
 * and other variants; and to expose all their sample-specific fields conveniently
 */

data class DiploidGenotype(val allele1: Int?, val allele2: Int?, val phased: Boolean) {
    fun validate(altAlleleCount: Int): DiploidGenotype {
        require((allele1 == null || allele1 <= altAlleleCount) && (allele2 == null || allele2 <= altAlleleCount), { this.toString() })
        return this
    }

    fun normalize(): DiploidGenotype {
        if (!phased && allele1 != null && (allele2 == null || allele1 > allele2)) {
            return DiploidGenotype(allele2, allele1, false)
        }
        return this
    }

    override fun toString(): String {
        return (if (allele1 == null) "." else allele1.toString()) + (if (phased) "|" else "/") + (if (allele2 == null) "." else allele2.toString())
    }
}

class UnpackedVcfRecord(val record: VcfRecord) {
    val altVariants = record.altsToVariants()
    val tsv: Array<String>
    val format: Array<String>
    val formatIndex: Map<String, Int>
    val sampleCount: Int
    private var lastSampleIndex: Int = -1
    private var lastSampleFields: Array<String> = emptyArray()

    init {
        tsv = record.line.split('\t').toTypedArray()
        sampleCount = tsv.size - VcfColumn.FIRST_SAMPLE.ordinal
        format = tsv[VcfColumn.FORMAT.ordinal].split(':').toTypedArray()
        formatIndex = format.mapIndexed { idx, field -> field to idx }.toMap()
        check(format[0] == "GT")
    }

    fun getSampleFields(sampleIndex: Int): Array<String> {
        if (sampleIndex != lastSampleIndex) {
            lastSampleFields = tsv[sampleIndex + VcfColumn.FIRST_SAMPLE.ordinal].split(':').toTypedArray()
            lastSampleIndex = sampleIndex
        }
        return lastSampleFields
    }

    fun getSampleField(sampleIndex: Int, fieldIndex: Int): String? {
        val fields = getSampleFields(sampleIndex)
        if (fieldIndex >= fields.size) {
            return null
        }
        val ans = fields[fieldIndex]
        return if (ans != "" && ans != ".") ans else null
    }

    fun getSampleField(sampleIndex: Int, field: String): String? {
        val idx = formatIndex.get(field)
        return if (idx == null) null else getSampleField(sampleIndex, idx)
    }

    fun getSampleFieldInt(sampleIndex: Int, field: String): Int? {
        return getSampleField(sampleIndex, field)?.toInt()
    }

    fun getSampleFieldInts(sampleIndex: Int, field: String): Array<Int?> {
        val fld = getSampleField(sampleIndex, field)
        if (fld == null) {
            return emptyArray()
        }
        return fld.split(',').map { if (it == "" || it == ".") null else it.toInt() }.toTypedArray()
    }

    fun getDiploidGenotype(sampleIndex: Int): DiploidGenotype {
        val gt = getSampleField(sampleIndex, 0)
        if (gt == null) {
            return DiploidGenotype(null, null, false)
        }
        var parts = gt.split('|')
        var phased = true
        require(parts.size <= 2)
        if (parts.size < 2) {
            parts = gt.split('/')
            require(parts.size == 2)
            phased = false
        }
        val allele1 = (if (parts[0] == "" || parts[0] == ".") null else parts[0].toInt())
        val allele2 = (if (parts[1] == "" || parts[1] == ".") null else parts[1].toInt())
        return DiploidGenotype(allele1, allele2, phased).validate(tsv[VcfColumn.ALT.ordinal].split(',').size)
    }

    fun getAltIndex(variant: Variant): Int {
        return altVariants.mapIndexed { idx, vt -> if (vt == variant) (idx + 1) else null }.firstNotNullOfOrNull { it } ?: -1
    }
}

class UnpackedVcfRecords(val aggHeader: AggVcfHeader, val variant: Variant, val callsetRecordsRow: Row, val vcfRecordsCompressed: Boolean) {
    val callsetId: Int
    val referenceBands: List<UnpackedVcfRecord>
    val variantRecords: List<UnpackedVcfRecord>
    val otherVariantRecords: List<UnpackedVcfRecord>

    init {
        callsetId = callsetRecordsRow.getAs<Int>("callsetId")
        var callsetRecords = if (vcfRecordsCompressed) {
            callsetRecordsRow.getList<ByteArray>(callsetRecordsRow.fieldIndex("callsetSnappyLines"))
                .map { parseVcfRecord(aggHeader.contigId, callsetId, String(Snappy.uncompress(it))) }
        } else {
            callsetRecordsRow.getList<String>(callsetRecordsRow.fieldIndex("callsetLines"))
                .map { parseVcfRecord(aggHeader.contigId, callsetId, it) }
        }
        callsetRecords = callsetRecords.sortedBy { it.range }

        // reference bands have no (non-symbolic) ALT alleles
        var parts = callsetRecords.map { UnpackedVcfRecord(it) }.partition { it.altVariants.filterNotNull().isEmpty() }
        referenceBands = parts.first
        // partition variant records based on whether they include the focal variant
        parts = parts.second.partition { it.altVariants.contains(variant) }
        variantRecords = parts.first
        otherVariantRecords = parts.second
    }
}
