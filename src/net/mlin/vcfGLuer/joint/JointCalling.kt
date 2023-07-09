package net.mlin.vcfGLuer.joint
import java.io.Serializable
import kotlin.text.StringBuilder
import net.mlin.vcfGLuer.data.*
import net.mlin.vcfGLuer.util.*
import org.apache.spark.SparkFiles
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.api.java.function.FlatMapGroupsFunction
import org.apache.spark.api.java.function.MapFunction
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
    val gt: JointGenotypeConfig,
    val formatFields: List<JointFormatField>
) : Serializable

/**
 * From local databases of variants & VCF records, generate spVCF header, pVCF line count, and
 * sorted DataFrame of Snappy-compressed spVCF lines
 */
fun jointCall(
    cfg: JointConfig,
    spark: SparkSession,
    aggHeader: AggVcfHeader,
    variantsDbSparkFile: String,
    vcfFilenamesDF: Dataset<Row>,
    pvcfHeaderMetaLines: List<String>,
    sparseEntryCount: LongAccumulator? = null
): Triple<String, Long, Dataset<Row>> {
    // broadcast supporting data for joint calling; using JavaSparkContext to sidestep
    // kotlin-spark-api overrides
    val jsc = JavaSparkContext(spark.sparkContext)
    val aggHeaderB = jsc.broadcast(aggHeader)
    val fieldsGenB = jsc.broadcast(JointFieldsGenerator(cfg, aggHeader))

    // make big DataFrame of sparse joint genotypes for each sample and "frame" of variants.
    // variant discovery already assigned each variant to a frame number (frameno).
    val sparseGenotypesDF = vcfFilenamesDF.flatMap(
        FlatMapFunction<Row, Row> {
            generateJointCalls(
                cfg,
                aggHeaderB.value,
                fieldsGenB.value,
                SparkFiles.get(variantsDbSparkFile),
                it.getAs<Int>("callsetId"),
                it.getAs<String>("vcfFilename"),
                sparseEntryCount
            ).iterator()
        },
        RowEncoder.apply(
            StructType()
                .add("frameno", DataTypes.IntegerType, false)
                .add("sampleId", DataTypes.IntegerType, false)
                .add("sparseGenotypes", DataTypes.BinaryType, false)
        )
    )

    // group the sparse genotypes by frameno & generate spVCF text lines (Snappy-compressed)
    val spvcfLinesDF = sparseGenotypesDF
        .groupByKey(MapFunction<Row, Int> { it.getAs<Int>(0) }, Encoders.INT())
        .flatMapGroups(
            FlatMapGroupsFunction<Int, Row, Row> { frameno, sparseGenotypeFrames ->
                transposeSparseGenotypeFrames(
                    cfg,
                    aggHeaderB.value,
                    fieldsGenB.value,
                    SparkFiles.get(variantsDbSparkFile),
                    frameno,
                    sparseGenotypeFrames
                ).iterator()
            },
            RowEncoder.apply(
                StructType()
                    .add("variantId", DataTypes.IntegerType, false)
                    .add("rid", DataTypes.ShortType, false)
                    .add("beg", DataTypes.IntegerType, false)
                    .add("end", DataTypes.IntegerType, false)
                    .add("splitId", DataTypes.IntegerType, false)
                    .add("frameno", DataTypes.IntegerType, false)
                    .add("snappyLine", DataTypes.BinaryType, false)
            )
            // persist before sorting: https://stackoverflow.com/a/56310076
        ).persist(org.apache.spark.storage.StorageLevel.DISK_ONLY())

    val spvcfLinesSortedDF = spvcfLinesDF
        .sort("variantId").persist(org.apache.spark.storage.StorageLevel.DISK_ONLY())

    // Perform a count() to force pvcfLinesDF, ensuring it registers as an SQL query in the history
    // server before subsequent steps that will drop to RDD. This provides useful diagnostic info
    // that would otherwise go missing at RDD level.
    val spvcfLineCount = spvcfLinesSortedDF.count()
    spvcfLinesDF.unpersist()
    val spvcfHeader = jointHeader(cfg, aggHeader, pvcfHeaderMetaLines, fieldsGenB.value)
    return Triple(spvcfHeader, spvcfLineCount, spvcfLinesSortedDF)
}

/**
 * Generate the sparse joint genotypes for (each sample in) one callset
 */
fun generateJointCalls(
    cfg: JointConfig,
    aggHeader: AggVcfHeader,
    fieldsGen: JointFieldsGenerator,
    variantsDbFilename: String,
    callsetId: Int,
    vcfFilename: String,
    sparseEntryCount: LongAccumulator? = null
): Sequence<Row> = // [(frameno, sampleId, sparseGenotypes)]
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
                    yield(VariantsDbRow(rs))
                }
            }
        }
        // gVCF records sequence
        scanVcfRecords(aggHeader.contigId, vcfFilename).use { records ->
            val callsetSamples = aggHeader.callsetsDetails[callsetId].callsetSamples
            val frameEncoders = Array(callsetSamples.size) { SparseGenotypeFrameEncoder() }
            var lastFrameno = -1

            // collate the two sequences to generate GenotypingContexts with each variant and the
            // overlapping VCF records
            generateGenotypingContexts(variants, records).forEach { ctx ->
                val frameno = ctx.variantRow.frameno
                if (frameno != lastFrameno && lastFrameno >= 0) {
                    check(frameno == lastFrameno + 1)
                    // moving on to new frame: complete the previous frame
                    frameEncoders.forEachIndexed { sampleIndex, encoder ->
                        if (!encoder.vacuous) {
                            yield(
                                RowFactory.create(
                                    lastFrameno,
                                    callsetSamples[sampleIndex], // sampleId
                                    encoder.completeFrame()
                                )
                            )
                        } else {
                            encoder.reset()
                        }
                    }
                }
                lastFrameno = frameno
                // add to the current frame, repeating the prior ref-band-derived entry if
                // possible; otherwise generate the appropriate entry.
                frameEncoders.forEachIndexed { sampleIndex, encoder ->
                    if (!encoder.addRepeatIfPossible(ctx)) {
                        encoder.addGenotype(
                            ctx,
                            generateGenotypeAndFormatFields(
                                cfg,
                                fieldsGen,
                                ctx,
                                sampleIndex
                            )
                        )
                    }
                }
            }
            if (lastFrameno >= 0) {
                // complete the final frame
                frameEncoders.forEachIndexed { sampleIndex, encoder ->
                    if (!encoder.vacuous) {
                        yield(
                            RowFactory.create(
                                lastFrameno,
                                callsetSamples[sampleIndex], // sampleId
                                encoder.completeFrame()
                            )
                        )
                        sparseEntryCount?.let { it.add(encoder.totalRepeats) }
                    }
                }
            }
            check(GenomicSQLiteReadOnlyPool.distinctConnections() < 1000)
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
            val altIdx = it.getAltIndex(data.variantRow.variant)
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

    val altIdx = record.getAltIndex(data.variantRow.variant)
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

    if (data.callsetRecords.isEmpty()) {
        return "."
    }

    // count copies of other overlapping variants
    var otherOverlaps = countAltCopies(data.otherVariantRecords, sampleIndex)
    var refRanges = data.otherVariantRecords.map {
        check(it.record.range.overlaps(data.variantRow.variant.range))
        val gt = it.getDiploidGenotype(sampleIndex)
        if (gt.allele1 != null && gt.allele2 != null) it.record.range else null
    }.filterNotNull()

    // check if overlapping records and reference bands completely covered the focal variant range
    refRanges += data.referenceBands.filter {
        check(it.record.range.overlaps(data.variantRow.variant.range))
        val gt = it.getDiploidGenotype(sampleIndex)
        gt.allele1 == 0 && gt.allele2 == 0
    }.map { it.record.range }
    val refCoverage = data.variantRow.variant.range.subtract(refRanges).isEmpty()
    val refCall = if (refCoverage) 0 else null

    val allele1 = if (otherOverlaps > 0) genotypeOverlapSentinel(cfg.gt.overlapMode) else refCall
    val allele2 = if (otherOverlaps > 1) genotypeOverlapSentinel(cfg.gt.overlapMode) else refCall

    val gtOut = DiploidGenotype(allele1, allele2, false).normalize()
    val entry = gtOut.toString() + fieldsGen.generateFormatFields(data, sampleIndex, gtOut, null)
    return entry
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
 * Given the sparseGenotypeFrames gathered from all samples for this frameno, transpose them into
 * spVCF text lines (Snappy-compressed).
 *
 * Although we could easily generate complete spVCF frames instead of individual lines, doing so
 * might conveivably approach a 2G JVM limit and/or severely skew the Spark partitions. And in
 * variant discovery, we designed the framenos to ensure each split range will contain complete
 * frames (beginning with a spVCF checkpoint) after reassembling the individual lines.
 */
fun transposeSparseGenotypeFrames(
    cfg: JointConfig,
    aggHeader: AggVcfHeader,
    fieldsGen: JointFieldsGenerator,
    variantsDbFilename: String,
    frameno: Int,
    sparseGenotypeFrames: Iterator<Row> // [(frameno, sampleId, sparseGenotypes)]
): Sequence<Row> { // [(variantId, rid, beg, end, splitId, frameno, snappyLine)]
    // load this frame's variants from the broadcast database file
    val variants = ExitStack().use { cleanup ->
        val dbc = cleanup.add(
            GenomicSQLiteReadOnlyPool
                .get(variantsDbFilename)
                .getConnection()
        )
        check(GenomicSQLiteReadOnlyPool.distinctConnections() < 1000)
        val getVariants = cleanup.add(
            dbc.prepareStatement(
                """
                SELECT * FROM Variant INDEXED BY VariantFrameno
                WHERE frameno = ? ORDER BY variantId
                """
            )
        )
        getVariants.setInt(1, frameno)
        val rs = cleanup.add(getVariants.executeQuery())
        val lst = mutableListOf<VariantsDbRow>()
        var lastVariantId = -1
        while (rs.next()) {
            val row = VariantsDbRow(rs)
            check(lastVariantId < 0 || row.variantId == lastVariantId + 1)
            lastVariantId = row.variantId
            lst.add(row)
        }
        lst
    }

    // initialize decoders for each sample from sparseGenotypeFrames
    var totalFrameCount = 0
    val framesBySampleId = sparseGenotypeFrames.asSequence().associateBy(
        {
            check(it.getAs<Int>("frameno") == frameno)
            totalFrameCount += 1
            it.getAs<Int>("sampleId")
        },
        { it.getAs<ByteArray>("sparseGenotypes") }
    )
    require(
        totalFrameCount == framesBySampleId.size,
        { "colliding sparse genotype frames; check input files for duplication/overlap" }
    )
    val vacuousFrame = vacuousSparseGenotypeFrame(variants.size)
    val decoders = Array(aggHeader.samples.size) {
        decodeSparseGenotypeFrame(framesBySampleId.getOrDefault(it, vacuousFrame)).iterator()
    }

    // for each variant, decode all the genotypes & assemble the spVCF line
    return sequence {
        if (variants.isNotEmpty()) {
            val checkpointVariant = variants.first().variant
            variants.forEach { vdbrow ->
                val entries = decoders.asSequence().map { it.next() }
                val snappyLine = assembleJointLine(
                    cfg,
                    aggHeader,
                    fieldsGen,
                    checkpointVariant,
                    vdbrow.variant,
                    vdbrow.stats,
                    entries
                )
                yield(
                    RowFactory.create(
                        vdbrow.variantId,
                        vdbrow.variant.range.rid,
                        vdbrow.variant.range.beg,
                        vdbrow.variant.range.end,
                        vdbrow.splitId,
                        vdbrow.frameno,
                        snappyLine
                    )
                )
            }
        }
        decoders.forEach {
            check(!it.hasNext(), {
                "BUG: sparse genotype frames aren't consistent across samples"
            })
        }
    }
}

/**
 * Given the sparse genotype entries for a specific variant, assemble the spVCF line as a
 * compressed buffer. The Snappy compression is just to reduce the amount of data Spark has to
 * shuffle around to sort the lines afterwards.
 */
fun assembleJointLine(
    cfg: JointConfig,
    aggHeader: AggVcfHeader,
    fieldsGen: JointFieldsGenerator,
    checkpointVariant: Variant,
    variant: Variant,
    stats: VariantStats,
    entries: Sequence<String?>
): ByteArray {
    val checkpoint = variant.compareTo(checkpointVariant) == 0
    val info: MutableList<Pair<String, String>> = mutableListOf()
    if (!checkpoint) {
        info.add("spVCF_checkpointPOS" to checkpointVariant.range.beg.toString())
    }
    info.add("QUAL2" to (stats.qual2?.toString() ?: "."))
    // TODO: add fieldsGen.generateInfoFields()

    val buf = StringBuilder()
    buf.append(aggHeader.contigs[variant.range.rid.toInt()]) // CHROM
    buf.append('\t')
    buf.append(variant.range.beg.toString()) // POS
    buf.append("\t.\t") // ID
    buf.append(variant.ref) // REF
    buf.append('\t')
    buf.append(variant.alt) // ALT
    buf.append('\t')
    buf.append(stats.qual?.toString() ?: ".")
    buf.append('\t')
    buf.append("PASS") // FILTER
    buf.append('\t') // INFO
    buf.append(if (info.isEmpty()) "." else info.map { (k, v) -> "$k=$v" }.joinToString(";"))
    buf.append('\t')
    buf.append( // FORMAT
        (listOf("GT") + cfg.formatFields.map { it.name }).joinToString(":")
    )

    var entryCount = 0
    var sparseRun = 0
    entries.forEach { entry ->
        if (entry != null) {
            if (sparseRun > 0) {
                buf.append("\t\"")
                if (sparseRun > 1) {
                    buf.append(sparseRun.toString())
                }
                sparseRun = 0
            }
            buf.append('\t')
            buf.append(entry)
        } else {
            check(!checkpoint, { "BUG: checkpoint row should be fully dense" })
            sparseRun += 1
        }
        entryCount += 1
    }
    check(entryCount == aggHeader.samples.size)
    if (sparseRun > 0) {
        buf.append("\t\"")
        if (sparseRun > 1) {
            buf.append(sparseRun.toString())
        }
    }

    // generate compressed line for pVCF sorting
    return Snappy.compress(buf.toString().toByteArray())
}

/**
 * Write spVCF header
 */
fun jointHeader(
    cfg: JointConfig,
    aggHeader: AggVcfHeader,
    pvcfHeaderMetaLines: List<String>,
    fieldsGen: JointFieldsGenerator
): String {
    val ans = StringBuilder()
    ans.appendLine("##fileformat=spVCFv1;VCFv4.3")

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
    ans.appendLine(
        "##INFO=<ID=QUAL2,Number=1,Type=Integer,Description=" +
            "\"Second-rank variant quality score (after the first in the QUAL column)\">"
    )

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
