package net.mlin.vcfGLuer.data
import kotlin.math.floor
import kotlin.math.log10
import kotlin.math.min

/**
 * Number of distinct, unphased diploid genotypes for the given total # of alleles.
 */
fun diploidGenotypeCount(alleleCount: Int): Int {
    return (alleleCount + 1) * alleleCount / 2
}

/**
 * Order index of the unphased diploid genotype with the given two zero-based allele numbers.
 * (The order as in VCF Number=G fields; a/b and b/a are equivalent and normalized to nondecreasing)
 */
fun diploidGenotypeIndex(allele1: Int, allele2: Int): Int {
    require(allele1 >= 0 && allele2 >= 0)
    if (allele2 < allele1) {
        return (allele1 * (allele1 + 1) / 2) + allele2
    }
    return (allele2 * (allele2 + 1) / 2) + allele1
}

/**
 * Ordered list of distinct, unphased diploid genotypes for the given total # of alleles.
 */
fun diploidGenotypes(alleleCount: Int): List<Pair<Int, Int>> =
    (0 until alleleCount).map { b -> (0..b).map { a -> a to b } }.flatten()

/**
 * Given an unphased diploid genotype index, recover the constituent allele numbers.
 */
fun diploidGenotypeAlleles(genotypeIndex: Int): Pair<Int, Int> {
    val allele2 = ((kotlin.math.sqrt((8 * genotypeIndex + 1).toDouble()) - 1.0) / 2.0).toInt()
    val allele1 = genotypeIndex - allele2 * (allele2 + 1) / 2
    return (allele1 to allele2)
}

/**
 * GT field in a diploid VCF
 */
data class DiploidGenotype(val allele1: Int?, val allele2: Int?, val phased: Boolean) {
    fun validate(altAlleleCount: Int): DiploidGenotype {
        require(
            (allele1 == null || allele1 <= altAlleleCount) &&
                (allele2 == null || allele2 <= altAlleleCount),
            { this.toString() }
        )
        return this
    }

    fun normalize(): DiploidGenotype {
        if (!phased && allele1 != null && (allele2 == null || allele1 > allele2)) {
            return DiploidGenotype(allele2, allele1, false)
        }
        return this
    }

    override fun toString(): String {
        return (if (allele1 == null) "." else allele1.toString()) +
            (if (phased) "|" else "/") +
            (if (allele2 == null) "." else allele2.toString())
    }

    /**
     * Revise genotype by applying an allele frequency prior to genotype likelihoods.
     *
     * This is the GLenxus revise_genotypes feature, simplified by assuming the variant is
     * biallelic (so PL should have length 3). Revise 1/1 genotypes to 0/1 if the allele is too
     * rare, specifically if the phred-scaled allele frequency (times a calibration factor) is
     * greater than PL[1].
     */
    fun revise(
        PL: Array<Int?>,
        alleleFrequency: Float,
        calibrationFactor: Float
    ): Pair<DiploidGenotype, String?> {
        if (this.revisable() &&
            PL.size == 3 && PL.all { it != null } && PL[2] == 0
        ) {
            val PL01 = min(PL[0]!!, PL[1]!!) // collapse unusual case where PL[0] < PL[1]
            val prior = -10.0 * log10(alleleFrequency) * calibrationFactor
            if (PL01 < prior) {
                val revisedGQ = floor(prior - PL01).toInt()
                return DiploidGenotype(0, 1, false) to revisedGQ.toString()
            } else {
                val revisedGQ = floor(PL01 - prior).toInt()
                return this to revisedGQ.toString()
            }
        }
        return this to null
    }

    fun revisable(): Boolean {
        // optimization, so caller can decide whether to compute arguments to revise()
        return allele1 == 1 && allele2 == 1 && !phased
    }
}

fun testDiploidGenotypeRevise() {
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

fun testDiploidSubroutines() {
    for (alleleCount in 2..16) {
        val genotypes = diploidGenotypes(alleleCount)
        check(diploidGenotypeCount(alleleCount) == genotypes.size)
        genotypes.forEachIndexed { gt, (a, b) ->
            check(diploidGenotypeIndex(a, b) == gt)
            check(diploidGenotypeIndex(b, a) == gt)
            check(diploidGenotypeAlleles(gt) == a to b)
        }
    }
    testDiploidGenotypeRevise()
}
