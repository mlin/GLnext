import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.api.java.function.MapGroupsFunction
import org.apache.spark.sql.*
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
import org.apache.spark.util.LongAccumulator
import org.jetbrains.kotlinx.spark.api.*
import org.xerial.snappy.Snappy

/**
 * Joint-call variantsDF & vcfRecordsDF into sorted pVCF lines
 */
fun jointCall(
    spark: org.apache.spark.sql.SparkSession,
    aggHeader: AggVcfHeader,
    variantsDF: Dataset<Row>,
    vcfRecordsDF: Dataset<Row>,
    binSize: Int,
    pvcfRecordCount: LongAccumulator? = null,
    pvcfRecordBytes: LongAccumulator? = null
): Dataset<String> {
    val aggHeaderB = spark.sparkContext.broadcast(aggHeader)
    // joint-call each variant into a snappy-compressed pVCF line with GRange columns
    val pvcfToSort = joinVariantsAndVcfRecords(variantsDF, vcfRecordsDF, binSize).mapGroups(
        object : MapGroupsFunction<Row, Row, Row> {
            override fun call(variantRow: Row, callsetsData: Iterator<Row>): Row {
                val ans = jointCallVariant(aggHeaderB.value, variantRow, callsetsData)
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
    // sort pVCF lines by GRange & decompress
    return pvcfToSort
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
 *  |-- callsetSnappyLines: array (nullable = false)
 *  |    |-- element: binary (containsNull = false)
 * nb: the callsetSnappyLines are NOT sorted
 */
fun joinVariantsAndVcfRecords(variantsDF: Dataset<Row>, vcfRecordsDF: Dataset<Row>, binSize: Int): KeyValueGroupedDataset<Row, Row> {
    // explode the variants & records across the GRange bins they touch
    val binnedVariants = variantsDF.selectExpr("explode(GRangeBins(rid,beg,end,$binSize)) as bin", "*").alias("var")
    val binnedRecords = vcfRecordsDF.selectExpr("explode(GRangeBins(rid,beg,end,$binSize)) as bin", "*").alias("vcf")

    val joinDF =
        binnedVariants
            // join by GRange bin
            .join(binnedRecords, col("var.bin") eq col("vcf.bin"), "left")
            // filter joined bins by variant/VCF POS overlap
            .filter((col("var.beg") leq col("vcf.end")) and (col("var.end") geq col("vcf.beg")))
            // group overlappers by (variant, callsetId)
            .groupBy("var.rid", "var.beg", "var.end", "var.ref", "var.alt", "vcf.callsetId")
            // collect -distinct- VCF records (thus removing any binning-derived duplication)
            .agg(collect_set("vcf.snappyLine").alias("callsetSnappyLines"))

    // finally group the (variant, callsetId, callsetSnappyLines) items by variant
    // using groupByKey instead of (relational) groupBy so that we can mapGroups for joint calling.
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
fun jointCallVariant(aggHeader: AggVcfHeader, variantRow: Row, callsetsData: Iterator<Row>): Row {
    // extract variant
    val variantRange = GRange(variantRow.getAs<Short>("rid"), variantRow.getAs<Int>("beg"), variantRow.getAs<Int>("end"))
    val variant = Variant(variantRange, variantRow.getAs<String>("ref"), variantRow.getAs<String>("alt"))

    // prepare output record
    val recordTsv = Array<String>(aggHeader.samples.size + 9) { "." }
    var formatFieldIds = "GT"

    // for each callset
    for (callsetRow in callsetsData) {
        val callsetId = callsetRow.getAs<Int>("callsetId")

        // extract & sort the input VCF records overlapping the variant
        val callsetRecords =
            callsetRow.getList<ByteArray>(callsetRow.fieldIndex("callsetSnappyLines"))
                .map { parseVcfRecord(aggHeader.contigId, callsetId, String(Snappy.uncompress(it))) }
                .sortedBy { it.range }

        // generate genotype entry for each sample in the callset
        callsetGenotypes(aggHeader, variant, callsetId, callsetRecords).forEach {
            // check fieldIds vs formatFieldIds and extend if needed
            var longFieldIds = it.fieldIds
            var shortFieldIds = formatFieldIds
            if (longFieldIds.length < shortFieldIds.length) {
                longFieldIds = shortFieldIds.also { shortFieldIds = longFieldIds }
            }
            require(longFieldIds.startsWith(shortFieldIds), {
                val exemplarFilename = callsetExemplarFilename(aggHeader, callsetId)
                "$exemplarFilename FORMAT field order ${it.fieldIds} inconsistent with previous $formatFieldIds"
            })
            formatFieldIds = longFieldIds
            check(recordTsv[it.pvcfSampleIdx + 9] == ".", {
                "duplicate genotype entries ${variant.str(aggHeader.contigs)} ${aggHeader.samples[it.pvcfSampleIdx]}"
            })
            // fill in the output record
            recordTsv[it.pvcfSampleIdx + 9] = it.fields
        }
    }

    // fill out variant details
    recordTsv[0] = aggHeader.contigs[variant.range.rid.toInt()] // CHROM
    recordTsv[1] = variant.range.beg.toString() // POS
    // ID
    recordTsv[3] = variant.ref // REF
    recordTsv[4] = variant.alt // ALT
    // QUAL
    recordTsv[6] = "PASS" // FILTER
    // INFO
    recordTsv[8] = formatFieldIds // FORMAT

    // generate TSV & compress for pVCF sorting
    val snappyRecord = Snappy.compress(recordTsv.joinToString("\t").toByteArray())
    return RowFactory.create(variant.range.rid, variant.range.beg, variant.range.end, variant.alt, snappyRecord)
}

/**
 * Generate the pVCF genotype entry for each sample in a callset
 */
data class _GtEntry(val pvcfSampleIdx: Int, val fieldIds: String, val fields: String)
fun callsetGenotypes(aggHeader: AggVcfHeader, variant: Variant, callsetId: Int, callsetRecords: List<VcfRecord>): Sequence<_GtEntry> {
    return sequence {
        // Find callset record identical to variant
        val vtRecords = callsetRecords.filter { it.toVariant() == variant }
        if (vtRecords.size > 0) {
            require(vtRecords.size == 1, {
                val exemplarFilename = callsetExemplarFilename(aggHeader, callsetId)
                val variantStr = variant.str(aggHeader.contigs)
                "$exemplarFilename has multiple records for $variantStr"
            })
            val vtRecord = vtRecords.first()
            val tsv = vtRecord.line.split("\t").toTypedArray()

            // yield back the genotype entries (unchanged for now)
            aggHeader.callsetsDetails[callsetId].callsetSamples.forEachIndexed {
                inIdx, outIdx ->
                if (outIdx >= 0) {
                    yield(_GtEntry(outIdx, tsv[8], tsv[inIdx + 9]))
                }
            }
        }
    }
}
