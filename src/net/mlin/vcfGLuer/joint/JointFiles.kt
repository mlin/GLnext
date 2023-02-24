package net.mlin.vcfGLuer.joint
import java.io.OutputStreamWriter
import net.mlin.vcfGLuer.data.*
import net.mlin.vcfGLuer.util.*
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.function.Function2
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.util.LongAccumulator
import org.xerial.snappy.Snappy

fun writeJointFiles(
    contigs: Array<String>,
    splitBed: BedRanges,
    pvcfHeader: String,
    pvcfLines: Dataset<Row>,
    pvcfDir: String,
    pvcfRecordBytes: LongAccumulator? = null
): Int {
    val fs = getFileSystem(pvcfDir)
    require(fs.mkdirs(Path(pvcfDir)), { "output directory $pvcfDir mustn't already exist" })
    val partCount = pvcfLines.toJavaRDD().mapPartitionsWithIndex(
        Function2<Int, Iterator<Row>, Iterator<String>> { partIndex, rows ->
            writeJointPart(
                contigs,
                splitBed,
                partIndex,
                rows,
                pvcfDir,
                pvcfRecordBytes
            ).iterator()
        },
        false
    ).count().toInt()

    // write header
    fs.create(Path(pvcfDir, "00HEADER.bgz"), true).use {
        BGZFOutputStream(it).use {
            OutputStreamWriter(it, "UTF-8").use {
                it.write(pvcfHeader)
            }
        }
    }

    // lastly write EOF, atomically -- existence of zzEOF.bgz signals successful overall completion
    fs.create(Path(pvcfDir, ".wip.zzEOF.bgz"), true).use {
        it.write(htsjdk.samtools.util.BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK)
    }
    check(
        fs.rename(Path(pvcfDir, ".wip.zzEOF.bgz"), Path(pvcfDir, "zzEOF.bgz")),
        { "unable to finalize $pvcfDir/zzEOF.bgz" }
    )

    return partCount
}

fun writeJointPart(
    contigs: Array<String>,
    splitBed: BedRanges,
    partIndex: Int,
    rows: Iterator<Row>,
    pvcfDir: String,
    pvcfRecordBytes: LongAccumulator? = null
): List<String> {
    if (!rows.hasNext()) {
        return emptyList()
    }
    var row = rows.next()
    var range = GRange(row)
    val splitHits = splitBed.queryOverlapping(GRange(range.rid, range.beg, range.beg))
    require(
        splitHits.size == 1,
        {
            "--split-bed BED file regions are overlapping or not covering all contigs"
        }
    )
    val splitHit = splitHits.get(0)
    val splitId = splitHit.id
    val splitRange = GRange(range.rid, splitHit.beg, splitHit.end)

    val partPath = jointPartPath(contigs, pvcfDir, splitId, splitRange, splitBed.size, partIndex)
    val fs = getFileSystem(pvcfDir)
    fs.mkdirs(partPath.getParent())
    fs.create(partPath, true).use {
        BGZFOutputStream(it).use { bgzf ->
            while (true) {
                val line = Snappy.uncompress(row.getAs<ByteArray>("snappyLine"))
                pvcfRecordBytes?.let { it.add(line.size + 1L) }
                bgzf.write(line)
                bgzf.write(10) // \n

                if (!rows.hasNext()) {
                    break
                }
                row = rows.next()
                range = GRange(row)
            }
        }
    }
    return listOf(partPath.toString())
}

fun readSplitBed(contigId: Map<String, Short>, splitBed: String?): BedRanges {
    if (splitBed == null) {
        // synthesize one range per contig
        val contigRanges = contigId.values.map { GRange(it, 1, Int.MAX_VALUE) }
        return BedRanges(contigId, contigRanges.iterator())
    }
    return BedRanges(contigId, fileReaderDetectGz(splitBed))
}

fun jointPartPath(
    contigs: Array<String>,
    pvcfDir: String,
    splitId: Int,
    splitRange: GRange,
    totalSplits: Int,
    partIndex: Int
): Path {
    val contigName = contigs[splitRange.rid.toInt()]
    val splitPlaces = 1 + Math.log10(totalSplits.toDouble()).toInt()
    val splitSubdir = (
        (splitId + 1).toString().padStart(splitPlaces, '0') +
            "_" +
            if (splitRange.end < Int.MAX_VALUE) {
                "${contigName}_${splitRange.beg}_${splitRange.end}"
            } else {
                contigName
            }
        )

    // FIXME: don't hardcode 6
    val partIndexPadded = (partIndex + 1).toString().padStart(6, '0')
    return Path(Path(pvcfDir, splitSubdir), "part-$partIndexPadded.bgz")
}
