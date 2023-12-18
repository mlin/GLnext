// Helpers for generating all the pVCF QC fields (AD, PL, etc.)
package net.mlin.GLnext.joint
import java.io.Serializable
import kotlin.math.min
import net.mlin.GLnext.data.*

data class JointFormatField(val name: String, val header: String?, val impl: String?) :
    Serializable

abstract class JointFormatFieldImpl(val hdr: AggVcfHeader, val spec: JointFormatField) :
    Serializable {
    // redo JointHeader to consult JointFieldsGenerator
    protected open fun defaultHeaderLine(): String {
        // last-resort default: from the input VCF
        val headerLine = hdr.headerLines.get(VcfHeaderLineKind.FORMAT to spec.name)
        require(
            headerLine != null,
            {
                "Missing header for FORMAT field ${spec.name} " +
                    "(from input files or configuration entries)"
            }
        )
        return headerLine.lineText
    }
    fun headerLine(): String {
        return spec.header ?: defaultHeaderLine()
    }
    abstract fun generate(
        data: GenotypingContext,
        sampleIndex: Int,
        gt: DiploidGenotype,
        variantRecord: VcfRecordUnpacked?
    ): String?
}

/**
 * Verbatim copies FORMAT fields from variant record
 */
class CopiedFormatField(hdr: AggVcfHeader, spec: JointFormatField) :
    JointFormatFieldImpl(hdr, spec) {
    override fun generate(
        data: GenotypingContext,
        sampleIndex: Int,
        gt: DiploidGenotype,
        variantRecord: VcfRecordUnpacked?
    ): String? {
        return variantRecord?.getSampleField(sampleIndex, spec.name)
    }
}

class GQextra_FormatField(hdr: AggVcfHeader, spec: JointFormatField) :
    JointFormatFieldImpl(hdr, spec) {
    override fun generate(
        data: GenotypingContext,
        sampleIndex: Int,
        gt: DiploidGenotype,
        variantRecord: VcfRecordUnpacked?
    ): String? {
        // If variantRecord, copy its GQ.
        if (variantRecord != null) {
            return variantRecord.getSampleField(sampleIndex, "GQ")
        }
        // If only referenceBands, take their minimum GQ.
        if (data.otherVariantRecords.isEmpty() && data.referenceBands.isNotEmpty()) {
            var minGQ = Int.MAX_VALUE
            var gqRanges: MutableList<GRange> = mutableListOf()
            data.referenceBands.forEach {
                val gq = it.getSampleFieldInt(sampleIndex, "GQ")
                if (gq != null) {
                    minGQ = min(minGQ, gq)
                    gqRanges.add(it.record.range)
                }
            }
            if (minGQ >= 0 && minGQ < Int.MAX_VALUE &&
                data.variantRow.variant.range.subtract(gqRanges).isEmpty()
            ) {
                return minGQ.toString()
            }
        }
        // Otherwise, leave missing.
        return null
    }
}

class DP_FormatField(hdr: AggVcfHeader, spec: JointFormatField) :
    JointFormatFieldImpl(hdr, spec) {
    override fun generate(
        data: GenotypingContext,
        sampleIndex: Int,
        gt: DiploidGenotype,
        variantRecord: VcfRecordUnpacked?
    ): String? {
        // If variantRecord, copy its DP. Otherwise, take the minimum of DP/MED_DP/MIN_DP from
        // other overlapping records.
        // TODO: option to round down to power of two (if variantRecord == null)
        var minDP = Int.MAX_VALUE
        val records = (
            if (variantRecord != null) {
                listOf(variantRecord)
            } else {
                (data.otherVariantRecords + data.referenceBands)
            }
            )
        var dpRanges: MutableList<GRange> = mutableListOf()
        records.forEach {
            val dp = (
                it.getSampleFieldInt(sampleIndex, "DP")
                    ?: it.getSampleFieldInt(sampleIndex, "MED_DP")
                    ?: it.getSampleFieldInt(sampleIndex, "MIN_DP")
                )
            if (dp != null) {
                minDP = min(minDP, dp)
                dpRanges.add(it.record.range)
            }
        }
        if (minDP >= 0 && minDP < Int.MAX_VALUE &&
            data.variantRow.variant.range.subtract(dpRanges).isEmpty()
        ) {
            return minDP.toString()
        }
        return null
    }
}

class AD_FormatField(hdr: AggVcfHeader, spec: JointFormatField) : JointFormatFieldImpl(hdr, spec) {
    override fun generate(
        data: GenotypingContext,
        sampleIndex: Int,
        gt: DiploidGenotype,
        variantRecord: VcfRecordUnpacked?
    ): String? {
        if (variantRecord == null) {
            return null
        }
        val parsedAD = variantRecord.getSampleFieldInts(sampleIndex, "AD")
        val varIdx = variantRecord.getAltIndex(data.variantRow.variant)
        check(varIdx > 0)
        // TODO: third field if OverlapMode is symbolic
        val refAD = parsedAD.get(0)?.toString()
        val varAD = if (parsedAD.size > varIdx) parsedAD.get(varIdx)?.toString() else null
        if (refAD == null && varAD == null) {
            return null
        }
        return (refAD ?: ".") + "," + (varAD ?: ".")
    }
}

open class PL_FormatField(hdr: AggVcfHeader, spec: JointFormatField) : JointFormatFieldImpl(
    hdr,
    spec
) {
    override fun generate(
        data: GenotypingContext,
        sampleIndex: Int,
        gt: DiploidGenotype,
        variantRecord: VcfRecordUnpacked?
    ): String? {
        var ans: String? = null
        if (variantRecord != null) {
            val alleleCount = variantRecord.altVariants.size + 1 // REF
            val varIdx = variantRecord.getAltIndex(data.variantRow.variant)
            check(varIdx > 0)
            val parsedPL = variantRecord.getSampleFieldInts(sampleIndex, "PL")
            if (parsedPL.size == diploidGenotypeCount(alleleCount)) {
                // pVCF PL for zero copies: min gVCF PL of any genotype with zero copies
                // (min serving as an approximation of marginalizing their likelihoods)
                val pl0 = diploidGenotypes(variantRecord.altVariants.size + 1)
                    .filter { (a, b) -> a != varIdx && b != varIdx }
                    .map { (a, b) -> parsedPL.get(diploidGenotypeIndex(a, b)) }
                // pVCF PL for one copy: min gVCF PL of any genotype with one copy
                val pl1 = (0..variantRecord.altVariants.size)
                    .filter { it != varIdx }
                    .map { parsedPL.get(diploidGenotypeIndex(it, varIdx)) }
                // pVCF PL for two copies
                val pl2 = parsedPL.get(diploidGenotypeIndex(varIdx, varIdx))

                val ansPL = listOf(
                    if (pl0.any { it == null }) {
                        null
                    } else {
                        pl0.filterNotNull().minOrNull()
                    },
                    if (pl1.any { it == null }) {
                        null
                    } else {
                        pl1.filterNotNull().minOrNull()
                    },
                    pl2
                )
                // output PL vector if all entries are non-null and at least one entry equals zero
                if (!ansPL.any { it == null } && ansPL.contains(0)) {
                    ans = ansPL.map { it?.toString() ?: "." }.joinToString(",")
                }
            }
        }
        return ans
    }
}

class PLextra_FormatField(hdr: AggVcfHeader, spec: JointFormatField) : PL_FormatField(hdr, spec) {
    override fun generate(
        data: GenotypingContext,
        sampleIndex: Int,
        gt: DiploidGenotype,
        variantRecord: VcfRecordUnpacked?
    ): String? {
        super.generate(data, sampleIndex, gt, variantRecord)?.let { return it }
        // If only referenceBands, take PL from the one with the lowest GQ.
        if (data.otherVariantRecords.isEmpty() && data.referenceBands.isNotEmpty()) {
            var minGQ = Int.MAX_VALUE
            var minGQ_PL: String? = null
            var gqRanges: MutableList<GRange> = mutableListOf()
            data.referenceBands.forEach {
                val gq = it.getSampleFieldInt(sampleIndex, "GQ")
                if (gq != null) {
                    if (gq < minGQ) {
                        minGQ = gq
                        minGQ_PL = it.getSampleField(sampleIndex, "PL")
                    }
                    gqRanges.add(it.record.range)
                }
            }
            if (minGQ_PL != null &&
                data.variantRow.variant.range.subtract(gqRanges).isEmpty()
            ) {
                return minGQ_PL
            }
        }
        // Otherwise, leave missing.
        return null
    }
}

class OL_FormatField(hdr: AggVcfHeader, spec: JointFormatField) : JointFormatFieldImpl(hdr, spec) {
    protected override fun defaultHeaderLine(): String {
        return "FORMAT=<ID=OL,Number=1,Type=Integer," +
            "Description=\"Copy number of other overlapping ALT alleles\">"
    }
    override fun generate(
        data: GenotypingContext,
        sampleIndex: Int,
        gt: DiploidGenotype,
        variantRecord: VcfRecordUnpacked?
    ): String? {
        var overlapCount = 0
        (data.variantRecords + data.otherVariantRecords).forEach {
            val recGT = it.getDiploidGenotype(sampleIndex)
            if (recGT.allele1 != null && recGT.allele1 > 0 &&
                it.altVariants[recGT.allele1 - 1] != data.variantRow.variant
            ) {
                overlapCount++
            }
            if (recGT.allele2 != null && recGT.allele2 > 0 &&
                it.altVariants[recGT.allele2 - 1] != data.variantRow.variant
            ) {
                overlapCount++
            }
        }
        return if (overlapCount > 0) overlapCount.toString() else null
    }
}

class JointFieldsGenerator(val cfg: JointConfig, aggHeader: AggVcfHeader) {
    val formatImpls: List<JointFormatFieldImpl>

    init {
        formatImpls = cfg.formatFields.map {
            when (it.impl ?: it.name) {
                "DP" -> DP_FormatField(aggHeader, it)
                "AD" -> AD_FormatField(aggHeader, it)
                "PL" -> PL_FormatField(aggHeader, it)
                "PLextra" -> PLextra_FormatField(aggHeader, it)
                "OL" -> OL_FormatField(aggHeader, it)
                "GQextra" -> GQextra_FormatField(aggHeader, it)
                else -> CopiedFormatField(aggHeader, it)
            }
        }
    }

    val formatFieldCount get() = formatImpls.size

    /**
     * generate the FORMAT fields for one sample (and update any INFO field accumulators)
     */
    fun generateFormatFields(
        data: GenotypingContext,
        sampleIndex: Int,
        gt: DiploidGenotype,
        variantRecord: VcfRecordUnpacked?,
        overrideGQ: String? = null
    ): String {
        val fields = cfg.formatFields.mapIndexed { i, it ->
            if (overrideGQ != null && it.name == "GQ") {
                overrideGQ
            } else {
                formatImpls[i].generate(data, sampleIndex, gt, variantRecord)
            }
        }
        val maxNotNullIdx = fields.mapIndexed { i, v -> v?.let { i } }.filterNotNull().maxOrNull()
        if (maxNotNullIdx == null) {
            return ""
        }
        return ":" + fields.take(maxNotNullIdx + 1).map { it ?: "." }.joinToString(":")
    }

    /**
     * generate variant-level INFO fields (after generating format fields for all samples since last reset())
     */
    fun generateInfoFields(): String {
        return "."
    }

    fun reset() {}

    fun fieldHeaderLines(): List<String> {
        return formatImpls.map { it.headerLine() }
    }
}
