import net.mlin.iitj.IntegerIntervalTree

class GRangeIndex<T> : java.io.Serializable {
    private val forest: Array<Pair<ArrayList<T>, IntegerIntervalTree>>
    private val accessor: (item: T) -> GRange
    val count: Int

    constructor(
        inputItems: Iterator<T>,
        accessor: (item: T) -> GRange,
        alreadySorted: Boolean = false
    ) {
        val wipItems = ArrayList<ArrayList<T>>()
        // bucket items by rid
        inputItems.forEach {
            val range = accessor(it)
            while (wipItems.size <= range.rid) {
                wipItems.add(arrayListOf())
            }
            wipItems.get(range.rid.toInt()).add(it)
        }
        // build interval forest
        var counter = 0
        forest = wipItems.map { ridItems ->
            if (!alreadySorted) {
                ridItems.sortWith(compareBy({ accessor(it).beg }, { accessor(it).end }))
            }
            ridItems.trimToSize()
            val builder = IntegerIntervalTree.Builder()
            ridItems.forEach {
                // nb: interval trees are zero-based & half-open
                //     GRange is one-based & closed
                builder.add(accessor(it).beg - 1, accessor(it).end)
                counter++
            }
            check(builder.isSorted())
            ridItems to builder.build()
        }.toTypedArray()
        count = counter
        this.accessor = accessor
    }

    public val size get() = count

    protected fun consId(rid: Short, idx: Int): Long {
        check(rid >= 0 && idx >= 0)
        return (rid.toLong() shl 48) + idx.toLong()
    }

    public fun item(id: Long): T {
        val rid = (id shr 48).toInt()
        val idx = (id and 0xFFFFFFFFL).toInt()
        return forest[rid].first.get(idx)
    }

    public fun all(): Sequence<Long> {
        return sequence {
            forest.forEachIndexed { rid, (items, _) ->
                items.forEachIndexed { idx, _ -> yield(consId(rid.toShort(), idx)) }
            }
        }
    }

    public fun overlapping(query: GRange): Sequence<Long> {
        val ids: MutableList<Long> = mutableListOf()
        if (query.rid >= 0 && query.rid < forest.size) {
            forest[query.rid.toInt()].second.queryOverlap(query.beg - 1, query.end, {
                ids.add(consId(query.rid, it.id))
                true
            })
        }
        return ids.asSequence()
    }

    public fun containedBy(query: GRange): Sequence<Long> {
        val ids: MutableList<Long> = mutableListOf()
        if (query.rid >= 0 && query.rid < forest.size) {
            forest[query.rid.toInt()].second.queryOverlap(query.beg - 1, query.end, {
                if (query.beg < it.beg && it.end <= query.end) {
                    ids.add(consId(query.rid, it.id))
                }
                true
            })
        }
        return ids.asSequence()
    }

    public fun hasOverlapping(query: GRange): Boolean {
        if (query.rid < 0 || query.rid >= forest.size) {
            return false
        }
        return forest[query.rid.toInt()].second.queryOverlapExists(query.beg - 1, query.end)
    }

    public fun hasContaining(query: GRange): Boolean {
        var ans = false
        if (query.rid >= 0 && query.rid < forest.size) {
            forest[query.rid.toInt()].second.queryOverlap(query.beg - 1, query.end, {
                if (it.beg < query.beg && it.end >= query.end) {
                    ans = true
                    false
                } else {
                    true
                }
            })
        }
        return ans
    }

    /**
     * Given a GRange-sorted record stream: yield, for each indexed range that overlaps at least
     * one record, the indexed range ID and the list of overlapping records.
     */
    public fun <R> streamingJoin(
        sortedRecords: Iterator<R>,
        recordAccessor: (record: R) -> GRange
    ): Sequence<Pair<Long, List<R>>> {
        val activeSet: HashMap<Long, MutableList<R>> = hashMapOf()
        var lastBox: GRange? = null

        return sequence {
            sortedRecords.forEach { rec ->
                val range = recordAccessor(rec)
                val last = lastBox ?: range
                require((range.rid == last.rid && range.beg >= last.beg) || range.rid > last.rid, {
                    "GRangeIndex.streamingJoin: stream must be sorted"
                })
                // add record to the list for each overlapping range in the index (opening new
                // activeSet entries as needed)
                overlapping(range).forEach {
                    activeSet.getOrPut(it, { mutableListOf() }).add(rec)
                }
                // prune activeSet: yield+remove the records for any indexed ranges we've moved
                // beyond in the sorted record stream.
                // this linear pass assumes activeSet is small!
                activeSet.keys.toList().sorted().forEach { key ->
                    val activeRange = accessor(item(key))
                    if (activeRange.rid != range.rid || activeRange.end < range.beg) {
                        yield(Pair(key, activeSet.remove(key)!!))
                    }
                }
                lastBox = range
            }
            // finally, yield the records for all the still-active ranges
            activeSet.keys.toList().sorted().forEach { key ->
                yield(Pair(key, activeSet.remove(key)!!))
            }
        }
    }
}

fun indexBED(contigId: Map<String, Short>, bed: java.io.Reader): GRangeIndex<GRange> {
    val ranges = sequence {
        bed.useLines { lines ->
            lines.forEach {
                val tsv = it.splitToSequence('\t').take(3).toList().toTypedArray()
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
    return GRangeIndex(ranges.iterator(), { it }, alreadySorted = true)
}
