package net.mlin.vcfGLuer.data

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
}
