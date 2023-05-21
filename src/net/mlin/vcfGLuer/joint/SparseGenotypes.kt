package net.mlin.vcfGLuer.joint
import java.io.ByteArrayOutputStream
import kotlin.text.StringBuilder
import net.mlin.vcfGLuer.data.*
import net.mlin.vcfGLuer.util.*

/* Intermediate sparse representation for the joint-called genotypes from one sample. By this we
 * mean we've taken the sample gVCF and "projected" it onto the jointly-discovered variant list.
 *
 * The sparse genotypes for the joint variants are a series of String pVCF entries interleaved with
 * delimiters that may also indicate repetition of the previous pVCF entry. The repetition arises
 * when that previous pVCF entry was derived from a single gVCF reference band, and the following
 * joint variants are covered by that same reference band. (Assumes that only GT and DP are filled
 * in from reference bands.)
 *
 * This series effectively conveys part of a pVCF column. The joint-calling process generates these
 * for "frames" of 128 variants at a time, then groups corresponding frames across all samples to
 * generate the pVCF lines for each frame.
 */

class SparseGenotypeFrameEncoder {
    private val buffer = ByteArrayOutputStream()
    private var prevEmpty: Boolean = false
    private var prevRefBand: VcfRecordUnpacked? = null
    private var repeat: Int = 0
    private var totalRepeatCounter: Long = 0

    /**
     * Peek at the GenotypingContext for the next variant. If it just has the last-seen reference
     * band, then increment the current delimiter repeat and return true, in which case caller
     * should move on to the next variant. Otherwise, return false and caller should generate the
     * pVCF entry for this variant to addGenotype().
     */
    fun addRepeatIfPossible(ctx: GenotypingContext): Boolean {
        if ((prevEmpty && ctx.callsetRecords.isEmpty()) ||
            (prevRefBand != null && ctx.soleReferenceBand === prevRefBand)
        ) {
            repeat += 1
            require(repeat < 128) // we could relax with multiple consecutive delimiters
            return true
        }
        return false
    }

    /**
     * Following false return from addRepeatIfPossible(), add the generated pVCF entry for the
     * current variant.
     *
     * entry must be ASCII characters. Delimiter bytes are recognized by 1 in the high bit, and the
     * remaining 7 bits encode the repeat value in [0, 128).
     */
    fun addGenotype(ctx: GenotypingContext, entry: String) {
        require(entry.isNotEmpty() && entry.all { it.code < 128 })
        if (buffer.size() > 0) {
            // add delimiter with encoded repeat value
            buffer.write(128 + repeat)
            totalRepeatCounter += repeat
        }
        buffer.write(entry.toByteArray(Charsets.US_ASCII))
        prevEmpty = ctx.callsetRecords.isEmpty()
        prevRefBand = ctx.soleReferenceBand
        check(!prevEmpty || prevRefBand == null)
        repeat = 0
    }

    /**
     * Return the encoded genotypes at the end of the frame. The encoder may then be reused.
     */
    fun completeFrame(): ByteArray {
        if (buffer.size() > 0 && repeat > 0) {
            buffer.write(128 + repeat)
            totalRepeatCounter += repeat
        }
        val ans = buffer.toByteArray()
        buffer.reset()
        prevRefBand = null
        repeat = 0
        return ans
    }

    val totalRepeats: Long
        get() = totalRepeatCounter
}

/**
 * Decode the sparse genotypes, yielding a (dense) sequence of pVCF entries.
 */
fun decodeSparseGenotypeFrame(input: ByteArray): Sequence<String> {
    return sequence {
        val entryBuffer = StringBuilder()
        for (byte in input) {
            val ubyte = byte.toUByte().toInt()
            check(0 <= ubyte && ubyte < 256)
            if (ubyte >= 128) { // delimiter
                require(entryBuffer.isNotEmpty(), { "sparse genotypes corrupt" })
                val entry = entryBuffer.toString()
                entryBuffer.clear()
                repeat(ubyte - 127) { // 1 if repeats = 0 / ubyte = 128
                    yield(entry)
                }
            } else { // one character of a pVCF entry
                entryBuffer.append(byte.toChar())
            }
        }
        if (entryBuffer.isNotEmpty()) {
            yield(entryBuffer.toString())
        }
    }
}
