package net.mlin.GLnext.joint
import kotlin.test.Test
import net.mlin.GLnext.data.*
import net.mlin.iitj.IntegerIntervalTree
import org.apache.commons.math3.distribution.GeometricDistribution
import org.apache.commons.math3.distribution.UniformIntegerDistribution
import org.apache.commons.math3.random.JDKRandomGenerator

class GenotypingContextTest {
    /**
     * Test the tricky logic in generateGenotypingContexts() to stream all the VCF records
     * overlapping each variant.
     */
    @Test
    fun testGenerate() {
        val domain = 100000
        val rng = JDKRandomGenerator()
        rng.setSeed(42)
        val unif = UniformIntegerDistribution(rng, 1, domain)
        val geom = GeometricDistribution(rng, 0.10)

        for (N in listOf(domain / 10, domain, domain * 10)) {
            val iitBuilder = IntegerIntervalTree.Builder()
            val records = (0 until N).map {
                val pos = unif.sample()
                val len = geom.sample() + 1
                val rec = if (it % 2 == 0) {
                    mockVariantRecord(pos, len)
                } else {
                    mockReferenceBand(pos, len)
                }
                check(rec.range.beg == pos && rec.range.end == pos + len - 1)
                rec
            }.sortedBy { it.range.beg }
            val iitTable = records.map {
                iitBuilder.add(it.range.beg, it.range.end + 1) to it
            }.toMap()
            val iit = iitBuilder.build()

            val variants = (0 until domain).map {
                mockVariantDbRow(unif.sample(), geom.sample() + 1)
            }.sortedBy { it.variant.range }

            val variantsSeen: MutableList<VariantsDbRow> = mutableListOf()
            generateGenotypingContexts(variants.asSequence(), records.iterator()).forEach {
                variantsSeen.add(it.variantRow)
                val iitHits = iit.queryOverlap(
                    it.variantRow.variant.range.beg,
                    it.variantRow.variant.range.end + 1
                ).map {
                    iitTable[it.id]
                }
                val ctxHits = it.callsetRecords.map { it.record }
                check(ctxHits.toSet() == iitHits.toSet())
            }
            check(variantsSeen == variants)
        }
    }

    val contigId = mapOf("chr1" to 0.toShort())

    fun mockVariantRecord(pos: Int, len: Int): VcfRecord {
        val ref = "A".repeat(len)
        return parseVcfRecord(contigId, "chr1\t${pos}\t.\t${ref}\tC\t.\tPASS\t.\tGT\t0/1")
    }

    fun mockReferenceBand(pos: Int, len: Int): VcfRecord {
        return parseVcfRecord(
            contigId,
            "chr1\t${pos}\t.\tA\t<*>\t.\tPASS\tEND=${pos + len - 1}\tGT\t0/0"
        )
    }

    fun mockVariantDbRow(pos: Int, len: Int): VariantsDbRow {
        val variant = Variant(GRange(0, pos, pos + len - 1), "A".repeat(len), "C")
        val stats = VariantStats(1, 0, 0)
        return VariantsDbRow(variant, stats, 0, 0, 0)
    }
}
