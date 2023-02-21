package net.mlin.vcfGLuer.joint
import java.io.OutputStreamWriter
import net.mlin.vcfGLuer.util.*
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.function.Function2
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.util.LongAccumulator
import org.xerial.snappy.Snappy

fun writeJointFiles(
    pvcfHeader: String,
    pvcfLines: Dataset<Row>,
    pvcfDir: String,
    pvcfRecordBytes: LongAccumulator? = null
): Int {
    val fs = getFileSystem(pvcfDir)
    require(fs.mkdirs(Path(pvcfDir)), { "output directory $pvcfDir mustn't already exist" })
    val partCount = pvcfLines.toJavaRDD().mapPartitionsWithIndex(
        Function2<Int, Iterator<Row>, Iterator<String>> { partIndex, rows ->
            val partIndexPadded = partIndex.toString().padStart(6, '0')
            val partPath = Path(pvcfDir, "part-$partIndexPadded.bgz")
            val fs = getFileSystem(pvcfDir)
            fs.create(partPath).use {
                BGZFOutputStream(it).use { bgzf ->
                    rows.forEach { row ->
                        val line = Snappy.uncompress(row.getAs<ByteArray>("snappyLine"))
                        pvcfRecordBytes?.let { it.add(line.size + 1L) }
                        bgzf.write(line)
                        bgzf.write(10) // \n
                    }
                }
            }
            listOf(partPath.toString()).iterator()
        },
        false
    ).count().toInt()

    fs.create(Path(pvcfDir, "00HEADER.bgz"), true).use {
        BGZFOutputStream(it).use {
            OutputStreamWriter(it, "UTF-8").use {
                it.write(pvcfHeader)
            }
        }
    }

    fs.create(Path(pvcfDir, ".wip.zzEOF.bgz"), true).use {
        it.write(htsjdk.samtools.util.BlockCompressedStreamConstants.EMPTY_GZIP_BLOCK)
    }
    check(
        fs.rename(Path(pvcfDir, ".wip.zzEOF.bgz"), Path(pvcfDir, "zzEOF.bgz")),
        { "unable to finalize $pvcfDir/zzEOF.bgz" }
    )

    return partCount
}
