package net.mlin.vcfGLuer.joint
import java.io.OutputStreamWriter
import net.mlin.vcfGLuer.data.*
import net.mlin.vcfGLuer.util.*
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function2
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.xerial.snappy.Snappy

data class PartWritten(
    val path: String,
    val splitId: Int,
    val partIndex: Int,
    var lineCount: Int,
    var byteCount: Long
)

// Write the sorted pVCF Dataset to an output p.vcf.gz file for each splitBed region
// NOTE: pvcfLines will be unpersist-ed by side-effect
fun writeJointFiles(
    logger: Logger,
    contigs: Array<String>,
    splitBed: BedRanges,
    pvcfHeader: String,
    pvcfLines: Dataset<Row>,
    pvcfDir: String,
    variantCount: Int
) {
    val fs = getFileSystem(pvcfDir)
    require(fs.mkdirs(Path(pvcfDir)), { "output directory $pvcfDir mustn't already exist" })

    // write file parts (split by both spark partition and splitBed)
    val (headerPath, eofPath, partsWritten) = writeAllJointFileParts(
        contigs,
        splitBed,
        pvcfHeader,
        pvcfLines,
        Path(pvcfDir, "_parts").toString(),
        variantCount,
        fs
    )

    val totalBytes = partsWritten.map { it.byteCount }.sum()
    logger.info(
        "wrote ${totalBytes.pretty()} pVCF record bytes in" +
            " ${partsWritten.size} parts; concatenating parts to pVCF files..."
    )

    // group the parts by splitBed region
    val partsPerSplit = partsWritten.groupBy {
        it.splitId
    }.entries.map { it.value.sortedBy { it.partIndex } }

    // concatenate the parts from each splitBed region into a well-formed p.vcf.gz file
    val jsc = JavaSparkContext(pvcfLines.sparkSession().sparkContext())
    pvcfLines.unpersist()
    val pvcfFiles = jsc.parallelizeEvenly(partsPerSplit).map { pvcfFileParts ->
        check(pvcfFileParts.sortedBy { it.path } == pvcfFileParts)
        val pvcfPath = Path(
            pvcfDir,
            Path(pvcfDir).getName() + "_" +
                Path(pvcfFileParts.first().path).getParent().getName() +
                ".vcf.gz"
        )
        val plan = listOf(headerPath) + pvcfFileParts.map { it.path } + listOf(eofPath)
        getFileSystem(pvcfDir).concatNaive(pvcfPath, plan.map { Path(it) }.toTypedArray())
        pvcfPath to pvcfFileParts.map { it.lineCount }.sum()
    }.collect()

    // cross-check total line count
    check(pvcfFiles.map { it.second }.sum() == variantCount)

    // mark _SUCCESS
    fs.create(Path(pvcfDir, "_SUCCESS"), false).use {}

    logger.info("created ${pvcfFiles.size} output pVCF files under $pvcfDir")

    // TODO: clean up _parts?
}

// Write the sorted pVCF Dataset to a number of sorted output files.
//
// The splitting of the sorted Dataset across the output files is guided by the union of the Spark
// partitioning AND splitBed. Each file has sorted variants whose beg POS lies within exactly one
// splitBed region (but each splitBed region may have multiple sorted output files, according to
// the Spark partitioning).
fun writeAllJointFileParts(
    contigs: Array<String>,
    splitBed: BedRanges,
    pvcfHeader: String,
    pvcfLines: Dataset<Row>,
    partsDir: String,
    variantCount: Int,
    fs: FileSystem
): Triple<String, String, List<PartWritten>> {
    require(fs.mkdirs(Path(partsDir)), { "output directory $partsDir mustn't already exist" })
    val partsWritten = pvcfLines.toJavaRDD().mapPartitionsWithIndex(
        Function2<Int, Iterator<Row>, Iterator<PartWritten>> { partIndex, rows ->
            writeJointFileParts(
                contigs,
                splitBed,
                partIndex,
                rows,
                partsDir
            ).iterator()
        },
        false
    ).collect()

    // cross-check total line count
    check(partsWritten.map { it.lineCount }.sum() == variantCount)

    // write header
    val headerPath = Path(partsDir, "00HEADER.bgz")
    fs.create(headerPath, true).use {
        BGZFOutputStream(it).use {
            OutputStreamWriter(it, "UTF-8").use {
                it.write(pvcfHeader)
            }
        }
    }

    // write EOF
    val eofPath = Path(partsDir, "zzEOF.bgz")
    fs.create(eofPath, true).use {
        it.write(htsjdk.samtools.util.BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK)
    }

    return Triple(headerPath.toString(), eofPath.toString(), partsWritten)
}

// Write one Spark partition of pVCF lines out to one or more bgzipped files. The partition may
// generate multiple output files if it spans multiple splitBed ranges (chromosomes by default).
fun writeJointFileParts(
    contigs: Array<String>,
    splitBed: BedRanges,
    partIndex: Int,
    rows: Iterator<Row>,
    pvcfDir: String
): List<PartWritten> {
    val fs = getFileSystem(pvcfDir)
    val partsWritten: HashMap<String, PartWritten> = hashMapOf()

    // current open file
    var partWriting: PartWritten? = null
    var bgzf: BGZFOutputStream? = null
    try {
        // for each dataset row
        while (rows.hasNext()) {
            val row = rows.next()
            // figure out which splitBed region the record's POS falls into
            var range = GRange(row)
            val splitHits = splitBed.queryOverlapping(GRange(range.rid, range.beg, range.beg))
            require(
                splitHits.size == 1,
                {
                    "--split-bed BED file regions are overlapping or not covering all contigs"
                }
            )
            val splitHit = splitHits.first()

            // if we don't already have the part file for that region open, then open it now
            if (partWriting == null || partWriting.splitId != splitHit.id) {
                // close completed part file, if any
                if (partWriting != null) {
                    bgzf!!.close()
                    partsWritten.put(partWriting.path, partWriting)
                }
                bgzf = null
                partWriting = null

                // Begin writing file for this partIndex & split region.
                // We set overwrite=true in case the Spark task had to be retried. Otherwise, we
                // won't erroneously clobber an existing file because our partIndex is unique and
                // and we double-check that we're not circling back on a file that we already wrote
                // ourselves (which could only happen if the rows aren't sorted).
                val partPath = jointPartPath(
                    contigs,
                    pvcfDir,
                    splitHit.id,
                    GRange(range.rid, splitHit.beg, splitHit.end),
                    splitBed.size,
                    partIndex
                )
                partWriting = PartWritten(partPath.toString(), splitHit.id, partIndex, 0, 0L)
                check(!partsWritten.containsKey(partWriting.path))
                fs.mkdirs(partPath.getParent())
                val outfile = fs.create(partPath, true)
                try {
                    bgzf = BGZFOutputStream(outfile)
                } catch (exc: Exception) {
                    outfile.close()
                    throw exc
                }
            }
            check(bgzf != null && partWriting != null)

            // write the line into the open BGZFOutputStream
            val line = Snappy.uncompress(row.getAs<ByteArray>("snappyLine"))
            bgzf.write(line)
            bgzf.write(10) // \n
            partWriting.lineCount += 1
            partWriting.byteCount += line.size + 1L
        }
    } finally {
        bgzf?.close()
    }
    if (partWriting != null) {
        partsWritten.put(partWriting.path, partWriting)
    }
    return partsWritten.values.toList()
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
    return Path(Path(pvcfDir, splitSubdir), partIndexPadded)
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
