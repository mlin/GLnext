package net.mlin.vcfGLuer.data
import htsjdk.samtools.util.CloseableIterator
import net.mlin.vcfGLuer.util.fileReaderDetectGz

enum class VcfColumn {
    CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO, FORMAT, FIRST_SAMPLE
}

/**
 * Raw VCF record with the extracted GRange
 */
data class VcfRecord(val range: GRange, val line: String)

/**
 * Parse VCF text line into VcfRecord
 */
fun parseVcfRecord(contigId: Map<String, Short>, line: String): VcfRecord {
    val tsv = line.splitToSequence('\t').take(VcfColumn.INFO.ordinal + 1).toList().toTypedArray()
    return VcfRecord(parseVcfRecordRange(contigId, tsv), line)
}

/**
 * Extract the GRange from VCF tab-separated values
 */
fun parseVcfRecordRange(contigId: Map<String, Short>, tsv: Array<String>): GRange {
    val rid = contigId.getOrDefault(tsv[VcfColumn.CHROM.ordinal], -1)
    require(rid >= 0, { "unknown CHROM ${tsv[0]}" })
    val beg = tsv[VcfColumn.POS.ordinal].toInt()
    val endRef = beg + tsv[VcfColumn.REF.ordinal].length - 1
    val endInfo = parseVcfRecordInfo(tsv[VcfColumn.INFO.ordinal]).get("END")?.toInt()
    if (endInfo != null) {
        require(endInfo >= beg, { "invalid VCF INFO END $endInfo (< POS $endRef)" })
        return GRange(rid, beg, endInfo)
    }
    return GRange(rid, beg, endRef)
}

/**
 * Parse the INFO fields of a VCF record
 */
fun parseVcfRecordInfo(info: String): Map<String, String> {
    return hashMapOf(
        *info.split(';').map {
            val kv = it.split('=', limit = 2)
            check(kv.size == 1 || kv.size == 2)
            kv[0] to (if (kv.size > 1) kv[1] else "")
        }.toTypedArray()
    )
}

/**
 * Iterator over records in a VCF file, closeable to allow prompt cleanup even if only partially
 * consumed
 */
fun scanVcfRecords(
    contigId: Map<String, Short>,
    vcfFilename: String
): CloseableIterator<VcfRecord> {
    val reader = fileReaderDetectGz(vcfFilename)
    return object : CloseableIterator<VcfRecord> {
        var next: String? = null
        private fun update() {
            if (next == null) {
                var line = reader.readLine()
                while (line != null && (line.isEmpty() || line[0] == '#')) {
                    line = reader.readLine()
                }
                next = line
            }
        }
        override fun hasNext(): Boolean {
            update()
            return next != null
        }
        override fun next(): VcfRecord? {
            update()
            val line = next ?: throw NoSuchElementException()
            next = null
            return parseVcfRecord(contigId, line)
        }
        override fun remove() {
            throw UnsupportedOperationException()
        }
        override fun close() {
            next = null
            reader.close()
        }
    }
}

/**
 * Lightly-unpacked VcfRecord with accessors
 */
class VcfRecordUnpacked(val record: VcfRecord) {
    val tsv: Array<String>

    // normalized Variant for each ALT allele, or null for symbolic ALT alleles
    // (Index 0 in the array is GT allele "1")
    val altVariants: Array<Variant?>
    val format: Array<String>
    val formatIndex: Map<String, Int>
    val sampleCount: Int
    private var lastSampleIndex: Int = -1
    private var lastSampleFields: Array<String> = emptyArray()

    init {
        tsv = record.line.split('\t').toTypedArray()
        sampleCount = tsv.size - VcfColumn.FIRST_SAMPLE.ordinal
        format = tsv[VcfColumn.FORMAT.ordinal].split(':').toTypedArray()
        formatIndex = format.mapIndexed { idx, field -> field to idx }.toMap()
        check(format[0] == "GT")

        val ref = tsv[VcfColumn.REF.ordinal]
        altVariants = tsv[VcfColumn.ALT.ordinal].split(',').map {
                alt ->
            if (alt == "." || alt == "*" || alt.startsWith('<')) {
                null
            } else {
                require(
                    ref.length == record.range.end - record.range.beg + 1 &&
                        !alt.contains('.') && !alt.contains('<'),
                    { "invalid variant ${tsv.joinToString("\t")} (END=${record.range.end})" }
                )
                Variant(record.range, ref, alt).normalize()
            }
        }.toTypedArray()
    }

    fun getSampleFields(sampleIndex: Int): Array<String> {
        if (sampleIndex != lastSampleIndex) {
            lastSampleFields = tsv[sampleIndex + VcfColumn.FIRST_SAMPLE.ordinal]
                .split(':').toTypedArray()
            lastSampleIndex = sampleIndex
        }
        return lastSampleFields
    }

    fun getSampleField(sampleIndex: Int, fieldIndex: Int): String? {
        val fields = getSampleFields(sampleIndex)
        if (fieldIndex >= fields.size) {
            return null
        }
        val ans = fields[fieldIndex]
        return if (ans != "" && ans != ".") ans else null
    }

    fun getSampleField(sampleIndex: Int, field: String): String? {
        val idx = formatIndex.get(field)
        return if (idx == null) null else getSampleField(sampleIndex, idx)
    }

    fun getSampleFieldInt(sampleIndex: Int, field: String): Int? {
        return getSampleField(sampleIndex, field)?.toInt()
    }

    fun getSampleFieldInts(sampleIndex: Int, field: String): Array<Int?> {
        val fld = getSampleField(sampleIndex, field)
        if (fld == null) {
            return emptyArray()
        }
        return fld.split(',')
            .map { if (it == "" || it == ".") null else it.toInt() }
            .toTypedArray()
    }

    fun getDiploidGenotype(sampleIndex: Int): DiploidGenotype {
        val gt = getSampleField(sampleIndex, 0)
        if (gt == null) {
            return DiploidGenotype(null, null, false)
        }
        var parts = gt.split('|')
        var phased = true
        require(parts.size <= 2)
        if (parts.size < 2) {
            parts = gt.split('/')
            require(parts.size == 2)
            phased = false
        }
        val allele1 = (if (parts[0] == "" || parts[0] == ".") null else parts[0].toInt())
        val allele2 = (if (parts[1] == "" || parts[1] == ".") null else parts[1].toInt())
        return DiploidGenotype(allele1, allele2, phased)
            .validate(tsv[VcfColumn.ALT.ordinal].split(',').size)
    }

    fun getAltIndex(variant: Variant): Int {
        return altVariants.mapIndexed {
                idx, vt ->
            if (vt == variant) (idx + 1) else null
        }.firstNotNullOfOrNull { it } ?: -1
    }

    fun getSampleAltQualities(sampleIndex: Int): Array<Int?> {
        // Compute the quality score for existence of each ALT allele in the given sample.
        // We define this as:
        //     min(PL of genotypes with no copies of the allele)
        //   - min(PL of genotypes with at least one copy of the allele)
        //   (but not less than zero)
        val PL = getSampleFieldInts(sampleIndex, "PL")
        val genotypeCount = diploidGenotypeCount(altVariants.size + 1)

        return altVariants.mapIndexed { i, vt ->
            val altIndex = i + 1

            var score_with = Int.MAX_VALUE
            var score_without = Int.MAX_VALUE
            for (genotypeIndex in 0 until genotypeCount) {
                val score = if (PL.size > genotypeIndex) {
                    PL[genotypeIndex]
                } else { null } ?: Int.MAX_VALUE
                val (allele1, allele2) = diploidGenotypeAlleles(genotypeIndex)
                if (allele1 == altIndex || allele2 == altIndex) {
                    score_with = kotlin.math.min(score_with, score)
                } else {
                    score_without = kotlin.math.min(score_without, score)
                }
            }

            if (score_without == Int.MAX_VALUE || score_with == Int.MAX_VALUE) {
                null
            } else if (score_without < score_with) {
                0
            } else {
                score_without - score_with
            }
        }.toTypedArray()
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

fun diploidGenotypeCount(alleleCount: Int): Int {
    return (alleleCount + 1) * alleleCount / 2
}

fun diploidGenotypeIndex(allele1: Int, allele2: Int): Int {
    require(allele1 >= 0 && allele2 >= 0)
    if (allele2 < allele1) {
        return (allele1 * (allele1 + 1) / 2) + allele2
    }
    return (allele2 * (allele2 + 1) / 2) + allele1
}

fun diploidGenotypeAlleles(genotypeIndex: Int): Pair<Int, Int> {
    val allele2 = ((kotlin.math.sqrt((8 * genotypeIndex + 1).toDouble()) - 1.0) / 2.0).toInt()
    val allele1 = genotypeIndex - allele2 * (allele2 + 1) / 2
    check(diploidGenotypeIndex(allele1, allele2) == genotypeIndex)
    check(diploidGenotypeIndex(allele2, allele1) == genotypeIndex)
    return (allele1 to allele2)
}
