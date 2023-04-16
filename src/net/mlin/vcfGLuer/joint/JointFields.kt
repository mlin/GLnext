// Helpers for generating all the pVCF QC fields (AD, PL, etc.)
package net.mlin.vcfGLuer.joint
import java.io.Serializable
import kotlin.math.min
import net.mlin.vcfGLuer.data.*

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
            data.variant.range.subtract(dpRanges).isEmpty()
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
        val varIdx = variantRecord.getAltIndex(data.variant)
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

class PL_FormatField(hdr: AggVcfHeader, spec: JointFormatField) : JointFormatFieldImpl(hdr, spec) {
    override fun generate(
        data: GenotypingContext,
        sampleIndex: Int,
        gt: DiploidGenotype,
        variantRecord: VcfRecordUnpacked?
    ): String? {
        var ans: String? = null
        if (variantRecord != null) {
            val varIdx = variantRecord.getAltIndex(data.variant)
            check(varIdx > 0)
            val parsedPL = variantRecord.getSampleFieldInts(sampleIndex, "PL")
            val i11 = varIdx * (varIdx + 1) / 2 + varIdx // parsedPL index of varIdx/varIdx
            if (parsedPL.size > i11) {
                val ansPL = listOf(
                    parsedPL.get(0)?.toString(), // 0/0
                    parsedPL.get(varIdx)?.toString(), // 0/1
                    parsedPL.get(i11)?.toString() // 1/1
                )
                // require 0 to be among the lifted values
                if (ansPL.filterNotNull().size > 0 && ansPL.contains("0")) {
                    ans = ansPL.map { it ?: "." }.joinToString(",")
                }
            }
        }
        return ans
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
                it.altVariants[recGT.allele1 - 1] != data.variant
            ) {
                overlapCount++
            }
            if (recGT.allele2 != null && recGT.allele2 > 0 &&
                it.altVariants[recGT.allele2 - 1] != data.variant
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
                "OL" -> OL_FormatField(aggHeader, it)
                else -> CopiedFormatField(aggHeader, it)
            }
        }
    }

    /**
     * generate the FORMAT fields for one sample (and update any INFO field accumulators)
     */
    fun generateFormatFields(
        data: GenotypingContext,
        sampleIndex: Int,
        gt: DiploidGenotype,
        variantRecord: VcfRecordUnpacked?
    ): String {
        val fields = formatImpls.map { it.generate(data, sampleIndex, gt, variantRecord) }

        if (cfg.keepTrailingFields) {
            return ":" + fields.map { it ?: "." }.joinToString(":")
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
