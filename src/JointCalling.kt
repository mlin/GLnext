import org.apache.spark.sql.*
import org.apache.spark.util.LongAccumulator
import org.jetbrains.kotlinx.spark.api.*

/**
 * Container for the VCF records from one callset which overlap the given variant
 */
data class VariantAndCallsetVcfRecords(val variant: Variant, val callsetId: Int, val records: List<VcfRecord>)

/**
 * For each variant, all the overlapping VCF records from each callset
 */
typealias VariantsAndVcfRecords = KeyValueGroupedDataset<Variant, VariantAndCallsetVcfRecords>

/**
 * Compute VariantsAndVcfRecords from BinnedVariants & BinnedVcfRecords
 */
fun joinVariantsAndVcfRecords(variants: BinnedVariants, vcfRecords: BinnedVcfRecords): VariantsAndVcfRecords {
    // coarsely join bins of variants & vcf records
    var joined = variants.innerJoin(vcfRecords, variants.col("bin") eq vcfRecords.col("bin"))
    // filter the joined bins for actual overlap of variant/vcf genomic ranges
    return joined.filter(
        (joined.col("first.variant.range.beg") leq joined.col("second.record.range.end"))
            and (joined.col("first.variant.range.end") geq joined.col("second.record.range.beg"))
    )
        // group the overlappers by (variant, callsetId)
        .groupByKey {
            (bv, br) ->
            check(bv.variant.range.overlaps(br.record.range))
            bv.variant to br.record.callsetId
        }
        // compile the VCF records for each (variant, callsetId) into a range-sorted list without
        // bins or binning duplicates
        .mapGroups {
            k, items ->
            VariantAndCallsetVcfRecords(
                k.first,
                k.second,
                items.asSequence().map {
                    (bv, br) ->
                    check(k.first == bv.variant && k.second == br.record.callsetId)
                    br.record
                }.distinct().sortedBy { it -> it.range }.toList()
            )
        }
        // further group (by variant) the VariantAndCallsetVcfRecords for each callset
        .groupByKey { it.variant }
}

/**
 * Joint-call VariantsAndVcfRecords into pVCF text lines (unsorted)
 */
fun VariantsAndVcfRecords.jointCall(spark: org.apache.spark.sql.SparkSession, aggHeader: AggVcfHeader, pvcfRecordCount: LongAccumulator? = null): Dataset<Pair<Variant, String>> {
    val aggHeaderB = spark.sparkContext.broadcast(aggHeader)
    return mapGroups {
        variant, callsetsRecords ->
        val aggHeader_ = aggHeaderB.value

        val recordTsv = Array<String>(aggHeader_.samples.size + 9) { "." }
        var formatFieldIds = "GT"

        // for each callset
        callsetsRecords.forEach {
            val callsetData = it
            // generate genotype entry for each sample in the callset
            callsetGenotypes(aggHeader_, callsetData).forEach {
                // check fieldIds vs formatFieldIds and extend if needed
                var longFieldIds = it.fieldIds
                var shortFieldIds = formatFieldIds
                if (longFieldIds.length < shortFieldIds.length) {
                    longFieldIds = shortFieldIds.also { shortFieldIds = longFieldIds }
                }
                require(longFieldIds.startsWith(shortFieldIds), {
                    val exemplarFilename = callsetExemplarFilename(aggHeader_, callsetData.callsetId)
                    "$exemplarFilename FORMAT field order ${it.fieldIds} inconsistent with previous $formatFieldIds"
                })
                formatFieldIds = longFieldIds
                check(recordTsv[it.pvcfSampleIdx + 9] == ".", {
                    "duplicate genotype entries ${variant.str(aggHeader_.contigs)} ${aggHeader_.samples[it.pvcfSampleIdx]}"
                })
                recordTsv[it.pvcfSampleIdx + 9] = it.fields
            }
        }

        // fill out variant details & join TSV
        recordTsv[0] = aggHeader_.contigs[variant.range.rid.toInt()] // CHROM
        recordTsv[1] = variant.range.beg.toString() // POS
        // ID
        recordTsv[3] = variant.ref // REF
        recordTsv[4] = variant.alt // ALT
        // QUAL
        recordTsv[6] = "PASS" // FILTER
        // INFO
        recordTsv[8] = formatFieldIds // FORMAT

        pvcfRecordCount?.let { it.add(1L) }

        variant to recordTsv.joinToString("\t")
    }
}

/**
 * Generate the pVCF genotype entry for each sample in a callset
 */
data class _GtEntry(val pvcfSampleIdx: Int, val fieldIds: String, val fields: String)
fun callsetGenotypes(aggHeader: AggVcfHeader, callsetData: VariantAndCallsetVcfRecords): Sequence<_GtEntry> {
    return sequence {
        // Find callset record identical to variant
        val vtRecords = callsetData.records.filter { it.toVariant() == callsetData.variant }
        if (vtRecords.size > 0) {
            require(vtRecords.size == 1, {
                val exemplarFilename = callsetExemplarFilename(aggHeader, callsetData.callsetId)
                val variantStr = callsetData.variant.str(aggHeader.contigs)
                "$exemplarFilename has multiple records for $variantStr"
            })
            val vtRecord = vtRecords.first()
            val tsv = vtRecord.line.split("\t").toTypedArray()

            // yield back the genotype entries (unchanged for now)
            aggHeader.callsetsDetails[callsetData.callsetId].callsetSamples.forEachIndexed {
                inIdx, outIdx ->
                yield(_GtEntry(outIdx, tsv[8], tsv[inIdx + 9]))
            }
        }
    }
}

fun callsetExemplarFilename(aggHeader: AggVcfHeader, callsetId: Int): String {
    return java.io.File(aggHeader.callsetsDetails[callsetId].callsetFilenames.first()).getName()!!
}
