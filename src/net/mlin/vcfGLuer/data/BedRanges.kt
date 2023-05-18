package net.mlin.vcfGLuer.data
import net.mlin.iitj.IntegerIntervalTree

/**
 * Minimal in-memory data structure of a BED file indexed for overlap/containment queries. Can be
 * broadcasted efficiently.
 */
class BedRanges : java.io.Serializable {
    private val iit: Array<IntegerIntervalTree>
    private val totalCounts: IntArray // cumulative interval count, for id offset

    constructor(contigId: Map<String, Short>, ranges: Iterator<GRange>) {
        val rangesByRid = Array<MutableList<Pair<Int, Int>>>(contigId.size) { mutableListOf() }
        ranges.forEach {
            check(it.beg >= 1 && it.beg <= it.end)
            rangesByRid[it.rid.toInt()].add(it.beg to it.end)
        }

        var counter = 0
        val totCounts = mutableListOf(0)
        iit = rangesByRid.map {
                ridRanges ->
            val builder = IntegerIntervalTree.Builder()
            ridRanges
                .sortedWith(compareBy({ it.first }, { it.second }))
                .forEach {
                    builder.add(it.first - 1, it.second)
                    counter++
                }
            check(builder.isSorted())
            totCounts.add(counter)
            builder.build()
        }.toTypedArray()
        totalCounts = totCounts.toIntArray()
    }

    constructor(contigId: Map<String, Short>, bed: java.io.Reader) :
        this(contigId, parseBed(contigId, bed).iterator()) {}

    public val size get() = totalCounts[iit.size]

    public fun hasOverlapping(query: GRange): Boolean {
        // nb: interval trees are zero-based & half-open (as in BED)
        //     GRange is one-based & closed
        return iit[query.rid.toInt()].queryOverlapExists(query.beg - 1, query.end)
    }

    public fun queryOverlapping(query: GRange): List<IntegerIntervalTree.QueryResult> {
        val ridi = query.rid.toInt()
        return iit[ridi].queryOverlap(query.beg - 1, query.end).map {
            IntegerIntervalTree.QueryResult(it.beg + 1, it.end, it.id + totalCounts[ridi])
        }
    }

    public fun hasContaining(query: GRange): Boolean {
        var ans = false
        iit[query.rid.toInt()].queryOverlap(query.beg - 1, query.end, {
            if (it.beg < query.beg && it.end >= query.end) {
                ans = true
                false
            } else {
                true
            }
        })
        return ans
    }
}

fun parseBed(contigId: Map<String, Short>, bed: java.io.Reader): Sequence<GRange> {
    return sequence {
        bed.useLines { lines ->
            // nb: BED is whitespace-delimited, not tab-delimited!
            val delim = "\\s+".toRegex()
            lines.forEach {
                if (it.trim().isNotEmpty() && !it.startsWith("#")) {
                    val tsv = it.splitToSequence(delim).take(3).toList().toTypedArray()
                    check(tsv.size == 3, { "[BED] invalid line: $it" })
                    val rid = contigId.get(tsv[0])
                    check(rid != null, { "[BED] unknown chromosome: ${tsv[0]}" })
                    val beg = tsv[1].toInt()
                    val end = tsv[2].toInt()
                    check(end >= beg, { "[BED] invalid range: $it" })
                    yield(GRange(rid, beg + 1, end))
                }
            }
        }
    }
}
