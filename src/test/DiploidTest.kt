package net.mlin.GLnext.data
import kotlin.test.Test

class DiploidTest {
    @Test
    fun testSubroutines() {
        for (alleleCount in 2..16) {
            val genotypes = diploidGenotypes(alleleCount)
            check(diploidGenotypeCount(alleleCount) == genotypes.size)
            genotypes.forEachIndexed { gt, (a, b) ->
                check(diploidGenotypeIndex(a, b) == gt)
                check(diploidGenotypeIndex(b, a) == gt)
                check(diploidGenotypeAlleles(gt) == a to b)
            }
        }
    }

    @Test
    fun testGenotypeRevision() {
        var gt = DiploidGenotype(1, 1, false)
        val PL: Array<Int?> = arrayOf(20, 10, 0)
        var result = gt.revise(PL, 0.01f, 1.0f)
        check(result.first == DiploidGenotype(0, 1, false), { "$result" })
        check(result.second == "10", { "$result" })
        result = gt.revise(PL, 0.1f, 1.0f)
        check(result.first === gt, { "$result" })
        check(result.second == "0", { "$result" })
        result = gt.revise(PL, 0.01f, 0.5f)
        check(result.first === gt, { "$result" })
        check(result.second == "0", { "$result" })
        result = gt.revise(PL, 0.01f, 0.1f)
        check(result.first === gt, { "$result" })
        check(result.second == "7", { "$result" })

        result = gt.revise(arrayOf(10, 20, 0), 0.01f, 0.1f)
        check(result.first === gt, { "$result" })
        check(result.second == "7", { "$result" })

        result = gt.revise(arrayOf(20, 0, 10), 0.01f, 1.0f)
        check(result.first === gt, { "$result" })
        check(result.second == null, { "$result" })
        result = gt.revise(arrayOf(0), 0.01f, 1.0f)
        check(result.first === gt, { "$result" })
        check(result.second == null, { "$result" })
        result = gt.revise(arrayOf(null, 10, 0), 0.01f, 1.0f)
        check(result.first === gt, { "$result" })
        check(result.second == null, { "$result" })
        gt = DiploidGenotype(0, 1, false)
        result = gt.revise(PL, 0.01f, 1.0f)
        check(result.first === gt, { "$result" })
        check(result.second == null, { "$result" })
    }
}
