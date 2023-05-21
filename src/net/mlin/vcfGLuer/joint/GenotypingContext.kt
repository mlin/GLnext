package net.mlin.vcfGLuer.joint
import net.mlin.vcfGLuer.data.*
import net.mlin.vcfGLuer.util.*

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

    // if the context has exactly one reference band fully covering the variant, and no other
    // records
    val soleReferenceBand: VcfRecordUnpacked?

    init {
        // callsetRecords.forEach { check(it.record.range.overlaps(variant.range)) }
        // reference bands have no (non-symbolic) ALT alleles
        var parts = callsetRecords.partition { it.altVariants.filterNotNull().isEmpty() }
        referenceBands = parts.first
        // partition variant records based on whether they include the focal variant
        parts = parts.second.partition { it.altVariants.contains(variant) }
        variantRecords = parts.first
        otherVariantRecords = parts.second

        var band: VcfRecordUnpacked? = null
        if (referenceBands.size == 1 && variantRecords.isEmpty() && otherVariantRecords.isEmpty()) {
            val band2 = referenceBands.first()
            if (band2.record.range.contains(variant.range)) {
                band = band2
            }
        }
        soleReferenceBand = band
    }
}

/**
 * Collate variant & VCF record sequences (both range-sorted), producing the GenotypingContext for
 * each variant with the overlapping VCF records (if any).
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
    return variants.map { (variantId, variant) ->
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
        GenotypingContext(variantId, variant, hits)
    }
}
