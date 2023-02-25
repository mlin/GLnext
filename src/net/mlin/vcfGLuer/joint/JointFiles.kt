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

// Write the sorted pVCF Dataset to a number of sorted output files.
//
// The splitting of the sorted Dataset across the output files is guided by the union of the Spark
// partitioning AND splitBed. Each file has sorted variants whose beg POS lies within exactly one
// splitBed region (but each splitBed region may have multiple sorted output files, according to
// the Spark partitioning).
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

// Write one Spark partition of pVCF lines out to one or more bgzipped files. The partition may
// generate multiple output files if it crosses multiple splitBed ranges (chromosomes by default).
fun writeJointPart(
    contigs: Array<String>,
    splitBed: BedRanges,
    partIndex: Int,
    rows: Iterator<Row>,
    pvcfDir: String,
    pvcfRecordBytes: LongAccumulator? = null
): List<String> {
    val fs = getFileSystem(pvcfDir)
    val allPartPaths: MutableList<Path> = mutableListOf()
    var bgzf: BGZFOutputStream? = null
    try {
        // for each dataset row
        while (rows.hasNext()) {
            val row = rows.next()
            // derive the output part filename this pVCF line should go into, considering the
            // splitBed region its POS falls into and the Spark partIndex.
            var range = GRange(row)
            val splitHits = splitBed.queryOverlapping(GRange(range.rid, range.beg, range.beg))
            require(
                splitHits.size == 1,
                {
                    "--split-bed BED file regions are overlapping or not covering all contigs"
                }
            )
            val splitHit = splitHits.first()
            val partPath = jointPartPath(
                contigs,
                pvcfDir,
                splitHit.id,
                GRange(range.rid, splitHit.beg, splitHit.end),
                splitBed.size,
                partIndex
            )

            // if that's not the currently-open file, then open it now
            if (allPartPaths.isEmpty() || partPath != allPartPaths.last()) {
                bgzf?.close()
                check(!allPartPaths.contains(partPath)) // double-check row sorting
                fs.mkdirs(partPath.getParent())
                val outfile = fs.create(partPath, true)
                try {
                    bgzf = BGZFOutputStream(outfile)
                } catch (exc: Exception) {
                    outfile.close()
                    throw exc
                }
                allPartPaths.add(partPath)
                check(allPartPaths.contains(partPath))
            }
            check(bgzf != null)

            // write the line into the open BGZFOutputStream
            val line = Snappy.uncompress(row.getAs<ByteArray>("snappyLine"))
            bgzf.write(line)
            bgzf.write(10) // \n
            pvcfRecordBytes?.let { it.add(line.size + 1L) }
        }
    } finally {
        bgzf?.close()
    }
    return allPartPaths.map { it.toString() }
}

// Derive individual output filename given pvcfDir, splitBed region, and Spark partition index.
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

    val partIndexPadded = (partIndex + 1).toString().padStart(7, '0') // FIXME not to hardcode 7
    return Path(Path(pvcfDir, splitSubdir), "${splitSubdir}_$partIndexPadded.bgz")
}

// Load the split BED file into BedRanges. If no BED file specified, then synthesize one range per
// contig.
fun readSplitBed(contigId: Map<String, Short>, splitBed: String?): BedRanges {
    if (splitBed == null) {
        val contigRanges = contigId.values.map { GRange(it, 1, Int.MAX_VALUE) }
        return BedRanges(contigId, contigRanges.iterator())
    }
    return BedRanges(contigId, fileReaderDetectGz(splitBed))
}
