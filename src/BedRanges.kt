// Broadcast-able class for loading a BED file of calling ranges and filtering records & variants
// against them

import net.mlin.iitj.IntegerIntervalTree

class BedRanges : java.io.Serializable {
    private val iit: Array<IntegerIntervalTree>
    val count: Int

    constructor(contigId: Map<String, Short>, bed: java.io.Reader) {
        val ranges = Array<MutableList<Pair<Int, Int>>>(contigId.size) { mutableListOf() }

        bed.useLines {
            lines ->
            lines.forEach {
                val tsv = it.splitToSequence('\t').take(3).toList().toTypedArray()
                check(tsv.size == 3, { "[BED] invalid line: $it" })
                val rid = contigId.get(tsv[0])
                check(rid != null, { "[BED] unknown chromosome: ${tsv[0]}" })
                val beg = tsv[1].toInt()
                val end = tsv[2].toInt()
                check(end >= beg, { "[BED] invalid range: $it" })
                ranges[rid.toInt()].add(beg to end)
            }
        }

        var counter = 0
        iit = ranges.map {
            ridRanges ->
            val builder = IntegerIntervalTree.Builder()
            ridRanges
                .sortedWith(compareBy({ it.first }, { it.second }))
                .forEach {
                    builder.add(it.first, it.second)
                    counter++
                }
            check(builder.isSorted())
            builder.build()
        }.toTypedArray()
        count = counter
    }

    public val size get() = count

    public fun hasOverlapping(query: GRange): Boolean {
        // nb: interval trees are zero-based & half-open (as in BED)
        //     GRange is one-based & closed
        return iit[query.rid.toInt()].queryOverlapExists(query.beg - 1, query.end)
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
