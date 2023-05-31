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
 * This series effectively represents part of a spVCF column. The joint-calling process generates
 * these for "frames" of up to 128 variants at a time, then groups corresponding frames from all
 * samples to generate spVCF for each frame.
 */

class SparseGenotypeFrameEncoder {
    private val buffer = ByteArrayOutputStream()
    private var prevEmpty: Boolean = false
    private var prevRefBand: VcfRecordUnpacked? = null
    private var repeat: Int = 0
    private var totalRepeatCounter: Long = 0
    private var vacuous_: Boolean = true

    /**
     * Peek at the GenotypingContext for the next variant. If it just has the last-seen reference
     * band, then increment the current delimiter repeat and return true, in which case caller
     * should move on to the next variant. Otherwise, return false and caller should generate the
     * pVCF entry for this variant to addGenotype().
     */
    fun addRepeatIfPossible(ctx: GenotypingContext): Boolean {
        check(!prevEmpty || prevRefBand == null)
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
        if (!prevEmpty) {
            vacuous_ = false
        }
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
        prevEmpty = false
        prevRefBand = null
        repeat = 0
        vacuous_ = true
        return ans
    }

    /**
     * True iff the frame is empty or contains exclusively "." entries
     */
    val vacuous: Boolean
        get() = vacuous_

    val totalRepeats: Long
        get() = totalRepeatCounter
}

/**
 * Generate frame of `size` missing genotypes (.)
 */
fun vacuousSparseGenotypeFrame(size: Int): ByteArray {
    require(0 <= size && size <= 128)
    val buf = ByteArrayOutputStream()
    if (size > 0) {
        buf.write('.'.toInt())
        if (size > 1) {
            buf.write(127 + size)
        }
    }
    return buf.toByteArray()
}

/**
 * Decode the sparse genotypes, yielding a sequence of pVCF entries interleaved with nulls which
 * mean to repeat the last non-null entry.
 */
fun decodeSparseGenotypeFrame(input: ByteArray): Sequence<String?> {
    return sequence {
        val entryBuffer = StringBuilder()
        for (byte in input) {
            val ubyte = byte.toUByte().toInt()
            check(0 <= ubyte && ubyte < 256)
            if (ubyte >= 128) { // delimiter
                require(entryBuffer.isNotEmpty(), { "sparse genotypes corrupt" })
                val entry = entryBuffer.toString()
                entryBuffer.clear()
                yield(entry)
                repeat(ubyte - 128) {
                    yield(null)
                }
            } else { // one character of a pVCF entry
                entryBuffer.append(ubyte.toChar())
            }
        }
        if (entryBuffer.isNotEmpty()) {
            yield(entryBuffer.toString())
        }
    }
}
