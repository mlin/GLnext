
import net.mlin.iitj.IntegerIntervalTree
import org.apache.log4j.Logger
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.CoGroupFunction
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.api.java.function.MapGroupsFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.*
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
import org.apache.spark.util.LongAccumulator
import org.jetbrains.kotlinx.spark.api.*
import org.xerial.snappy.Snappy
import java.io.Serializable
import kotlin.math.pow

enum class GT_OverlapMode {
    MISSING,
    SYMBOLIC,
    REF
}

data class JointGenotypeConfig(val overlapMode: GT_OverlapMode) : Serializable
data class JointConfig(val keepTrailingFields: Boolean, val gt: JointGenotypeConfig, val formatFields: List<JointFormatField>) : Serializable

/**
 * Joint-call variantsDF & vcfRecordsDF into sorted pVCF lines
 */
fun jointCall(
    logger: Logger,
    cfg: JointConfig,
    spark: SparkSession,
    aggHeader: AggVcfHeader,
    variantsDF: Dataset<Row>,
    vcfRecordsDF: Dataset<Row>,
    pvcfHeaderMetaLines: List<String> = emptyList(),
    pvcfRecordCount: LongAccumulator? = null,
    pvcfRecordBytes: LongAccumulator? = null,
): Pair<String, JavaRDD<String>> {
    // prepare supporting data for joint calling; broadcast() using JavaSparkContext to sidestep
    // kotlin-spark-api overrides
    val jsc = JavaSparkContext(spark.sparkContext)
    val aggHeaderB = jsc.broadcast(aggHeader)
    val fieldsGenB = jsc.broadcast(JointFieldsGenerator(cfg, aggHeader))
    val pvcfHeader = jointVcfHeader(cfg, aggHeader, pvcfHeaderMetaLines, fieldsGenB.value)

    // perform the big range-join of variants to all overlapping VCF records
    VariantRanges(logger, aggHeader.contigs, variantsDF).deploy(spark)
    val vcfRecordsCompressed = vcfRecordsDF.columns().contains("snappyLine")
    val groupedRecords = groupVcfRecordsByOverlappingVariantId(variantsDF, vcfRecordsDF, vcfRecordsCompressed)

    // process groupedRecords into Snappy-compressed pVCF lines (+ Variant info)
    // the temporary Snappy compression is just to streamline the I/O burden of the ensuing sort
    val pvcfRowSchema = StructType()
        .add("rid", DataTypes.ShortType, false)
        .add("beg", DataTypes.IntegerType, false)
        .add("end", DataTypes.IntegerType, false)
        .add("ref", DataTypes.StringType, false)
        .add("alt", DataTypes.StringType, false)
        .add("snappyRecord", DataTypes.BinaryType, false)
    val pvcfRows = variantsDF
        // 'join' variants to the vid-keyed groupedRecords; using cogroup avoids having to
        // materialize all the variant details (rid, beg, end, ref, alt) within groupedRecords
        .groupByKey(MapFunction<Row, Long> { it.getAs<Long>("vid") }, Encoders.LONG())
        .cogroup(
            groupedRecords,
            CoGroupFunction<Long, Row, Row, Row> {
                vid, variantRows, callsetsData ->
                val variantRow = variantRows.next()
                check(variantRow.getAs<Long>("vid") == vid && !variantRows.hasNext())
                sequence {
                    // run joint-calling given the variant details and all overlapping records
                    yield(jointCallVariant(cfg, aggHeaderB.value, fieldsGenB.value, variantRow, callsetsData, vcfRecordsCompressed))
                    pvcfRecordCount?.let { it.add(1L) }
                }.iterator()
            },
            RowEncoder.apply(pvcfRowSchema)
        ).cache() // cache() before sorting: https://stackoverflow.com/a/56310076
    // Perform a count() to force pvcfRows, ensuring it registers as an SQL query in the history
    // server before next dropping to RDD. This provides useful diagnostic info that would
    // otherwise go missing. The log message also provides a progress marker.
    val pvcfRowCount = pvcfRows.count()
    logger.info("sorting $pvcfRowCount pVCF lines...")

    // sort pVCF rows by Variant and return the decompressed text of each line
    return pvcfHeader to pvcfRows
        .toJavaRDD()
        .sortBy(
            object : Function<Row, Variant> {
                override fun call(row: Row): Variant {
                    check(row.length() == 6)
                    val itArray = IntRange(0, 4).map { row.get(it) }.toTypedArray()
                    return Variant(GenericRowWithSchema(itArray, pvcfRowSchema))
                }
            },
            true,
            // Coalesce to fewer partitions now that the data size has been greatly reduced,
            // compared to what we dealt with in the big join. Thus output a smaller number of
            // reasonably-sized pVCF part files.
            // This is why we've dropped down to RDD -- Dataset.orderBy() doesn't provide direct
            // control of the output partitioning.
            jsc.defaultParallelism().toDouble().pow(2.0 / 3.0).toInt()
        ).map(
            object : Function<Row, String> {
                override fun call(it: Row): String {
                    check(it.length() == 6)
                    val ans = String(Snappy.uncompress(it.getAs<ByteArray>(5)))
                    pvcfRecordBytes?.let { it.add(ans.length + 1L) }
                    return ans
                }
            }
        )
}

class VariantRanges : Serializable {
    // This helper for joinVariantsAndVcfRecordsByRange() indexes all distinct variant GRanges,
    // associating an ID with each one, and registers Spark UDFs allowing fast overlap queries
    // (i.e. list IDs of all variant ranges overlapping a given query range). These IDs will
    // serve as the join key.
    private val iitByRid: Array<IntegerIntervalTree?>

    constructor(logger: Logger, contigs: Array<String>, variantsDF: Dataset<Row>) {
        // use spark to build the interval tree for each contig (rid)
        val treesByRidPre =
            // group distinct variant ranges by rid
            variantsDF.select("rid", "beg", "end").distinct().groupByKey(
                object : MapFunction<Row, Short> {
                    override fun call(row: Row): Short {
                        return row.getShort(0)
                    }
                },
                Encoders.SHORT()
            ).mapGroups(
                // build interval tree from each such group
                object : MapGroupsFunction<Short, Row, Row> {
                    override fun call(rid: Short, ranges: Iterator<Row>): Row {
                        val sortedRanges = ranges.asSequence().map {
                            check(it.getShort(0) == rid)
                            val beg = it.getInt(1)
                            val end = it.getInt(2)
                            beg to end
                        }.toList().sortedWith(compareBy({ it.first }, { it.second }))
                        check(!sortedRanges.isEmpty())
                        val builder = IntegerIntervalTree.Builder()
                        sortedRanges.forEach {
                            (beg, end) ->
                            // add 1 to end b/c IntegerIntervalTree uses the half-open convention
                            builder.add(beg, end + 1)
                        }
                        check(builder.isSorted())
                        return RowFactory.create(rid, builder.build().serializeToByteArray())
                    }
                },
                RowEncoder.apply(
                    StructType()
                        .add("rid", DataTypes.ShortType, false)
                        .add("iit", DataTypes.BinaryType, false)
                )
            )

        // collect interval trees to driver
        iitByRid = Array<IntegerIntervalTree?>(contigs.size) { null }
        var iitBytes = 0L
        var variantRangeCount = 0L
        treesByRidPre.collectAsList().map {
            it.getShort(0).toInt() to it.getAs<ByteArray>(1)!!
        }.forEach {
            (rid, buf) ->
            val iit = deserializeFromByteArray(buf) as IntegerIntervalTree
            check(iit.size() > 0 && iitByRid[rid] == null)
            iitByRid[rid] = iit
            iitBytes += buf.size.toLong()
            variantRangeCount += iit.size().toLong()
        }
        logger.info("VariantRanges count: $variantRangeCount")
        logger.info("VariantRanges broadcast: $iitBytes bytes")
    }

    // get ID of the exact given variant range (-1 if not found)
    fun variantRangeId(rid: Short, beg: Int, end: Int): Long {
        val id = iitByRid[rid.toInt()]!!.queryAnyExactId(beg, end + 1)
        if (id < 0) {
            return -1L
        }
        // add rid to high bytes of contig-specific ID
        return (rid.toLong() shl 48) + id.toLong()
    }

    // list IDs of all variant ranges overlapping the given arbitrary range
    fun overlappingVariantRangeIds(rid: Short, beg: Int, end: Int): List<Long> {
        val ans: MutableList<Long> = mutableListOf()
        iitByRid[rid.toInt()]!!.queryOverlapId(beg, end + 1, {
            ans.add((rid.toLong() shl 48) + it.toLong())
            true
        })
        return ans
    }

    // install UDFs for variantRangeId() and overlappingVariantRangeIds()
    fun deploy(spark: SparkSession): Broadcast<VariantRanges> {
        // broadcast interval trees to cluster (~12 bytes per distinct variant range)
        val variantRangesB = JavaSparkContext(spark.sparkContext).broadcast(this)
        // register functions using them
        spark.udf().register(
            "variantRangeId",
            object : UDF3<Short, Int, Int, Long> {
                override fun call(rid: Short?, beg: Int?, end: Int?): Long {
                    return variantRangesB.value.variantRangeId(rid!!, beg!!, end!!)
                }
            },
            DataTypes.LongType
        )
        spark.udf().register(
            "overlappingVariantRangeIds",
            object : UDF3<Short, Int, Int, LongArray> {
                override fun call(rid: Short?, beg: Int?, end: Int?): LongArray {
                    return variantRangesB.value.overlappingVariantRangeIds(rid!!, beg!!, end!!).toLongArray()
                }
            },
            ArrayType(DataTypes.LongType, false)
        )
        return variantRangesB
    }
}

/**
 * Join discovered variants with overlapping VCF records from each callset
 *
 * Keys of the returned groups are variant IDs
 * Values in the returned groups are:
 *  |-- callsetId: integer (nullable = true)
 *  |-- callsetLines: array (nullable = false)
 *  |    |-- element: binary (containsNull = false)
 * nb: the callsetLines are unsorted
 */
fun groupVcfRecordsByOverlappingVariantId(variantsDF: Dataset<Row>, vcfRecordsDF: Dataset<Row>, vcfRecordsCompressed: Boolean): KeyValueGroupedDataset<Long, Row> {
    // for each distinct variant range, list the overlapping VCF records in each callset
    var vridRecords = vcfRecordsDF.selectExpr(
        "explode(overlappingVariantRangeIds(rid,beg,end)) as vrid",
        "callsetId", if (vcfRecordsCompressed) "snappyLine" else "line"
    ).groupBy("vrid", "callsetId").agg(
        if (vcfRecordsCompressed) {
            collect_list("snappyLine").alias("callsetSnappyLines")
        } else {
            collect_list("line").alias("callsetLines")
        }
    ).alias("vcf")

    // join that to variant IDs via many-to-one link table from variant IDs to variant range IDs
    return variantsDF
        // assume the link table is small enough to broadcast (~20 bytes per variant), saving one
        // costly shuffle of vridRecords
        .selectExpr("vid", "variantRangeId(rid,beg,end) as vrid").alias("var").hint("broadcast")
        .join(vridRecords, col("var.vrid") eq col("vcf.vrid"))
        // group by variant ID
        .groupByKey(
            object : MapFunction<Row, Long> {
                override fun call(row: Row): Long {
                    return row.getAs<Long>("vid")
                }
            },
            Encoders.LONG()
        )
}

/**
 * Generate pVCF record (with GRange columns) for one variant given the data group
 */
fun jointCallVariant(cfg: JointConfig, aggHeader: AggVcfHeader, fieldsGen: JointFieldsGenerator, variantRow: Row, callsetsData: Iterator<Row>, vcfRecordsCompressed: Boolean): Row {
    // extract variant
    val variant = Variant(variantRow)

    // prepare output record
    val recordTsv = initializeOutputTsv(cfg, aggHeader, variant)

    // for each callset
    for (callsetRow in callsetsData) {
        // TODO: handle case where callsetRow is just null, from left join

        // parse the input VCF records overlapping the variant
        val unpackedRecords = VcfRecordsContext(aggHeader, variant, callsetRow, vcfRecordsCompressed)

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
    return RowFactory.create(variant.range.rid, variant.range.beg, variant.range.end, variant.ref, variant.alt, snappyRecord)
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

fun generateGenotypeAndFormatFields(cfg: JointConfig, fieldsGen: JointFieldsGenerator, data: VcfRecordsContext, sampleIndex: Int): String {
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

fun generateRefGenotypeAndFormatFields(cfg: JointConfig, fieldsGen: JointFieldsGenerator, data: VcfRecordsContext, sampleIndex: Int): String {
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

class VcfRecordsContext(val aggHeader: AggVcfHeader, val variant: Variant, val callsetRecordsRow: Row, val vcfRecordsCompressed: Boolean) {
    val callsetId: Int
    val referenceBands: List<VcfRecordUnpacked>
    val variantRecords: List<VcfRecordUnpacked>
    val otherVariantRecords: List<VcfRecordUnpacked>

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
        var parts = callsetRecords.map { VcfRecordUnpacked(it) }.partition { it.altVariants.filterNotNull().isEmpty() }
        referenceBands = parts.first
        // partition variant records based on whether they include the focal variant
        parts = parts.second.partition { it.altVariants.contains(variant) }
        variantRecords = parts.first
        otherVariantRecords = parts.second
    }
}
