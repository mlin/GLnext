import htsjdk.samtools.util.BlockCompressedOutputStream
import java.io.File
import java.io.OutputStream
import org.apache.hadoop.io.compress.CompressionOutputStream
import org.apache.hadoop.io.compress.Compressor
import org.apache.hadoop.io.compress.GzipCodec

/**
 * Compression codec wrapping BGZFOutputStream.
 *
 * Based on GLOW and Hadoop-BAM (Apache and MIT licenses, respectively):
 * https://github.com/projectglow/glow/blob/master/core/src/main/scala/io/projectglow/sql/util/BGZFCodec.scala
 * https://github.com/HadoopGenomics/Hadoop-BAM/blob/master/src/main/java/org/seqdoop/hadoop_bam/util/BGZFCodec.java
 *
 */
class BGZFCodec : GzipCodec() {
    override fun createOutputStream(sink: OutputStream): CompressionOutputStream {
        return BGZFOutputStream(sink)
    }

    override fun createOutputStream(
        sink: OutputStream,
        moot: Compressor?
    ): CompressionOutputStream {
        return createOutputStream(sink)
    }

    override fun getCompressorType(): Class<out Compressor>? {
        return null
    }

    override fun createCompressor(): Compressor? {
        return null
    }

    override fun getDefaultExtension(): String {
        return ".bgz"
    }
}

fun BGZFCodecClassName(): String {
    return BGZFCodec::class.java.getCanonicalName()
}

fun compressionCodecsWithBGZF(): String {
    return listOf(
        "org.apache.hadoop.io.compress.DefaultCodec",
        "org.apache.hadoop.io.compress.SnappyCodec",
        "org.apache.hadoop.io.compress.Lz4Codec",
        BGZFCodecClassName()
    ).joinToString(", ")
}

/**
 * CompressionOutputStream for writing out a Dataset<String> to BGZF shards. The shards omit the
 * BGZF EOF marker so that they can later be concatenated into one giant file.
 *
 * Based on GLOW's (Apache-licensed):
 * https://github.com/projectglow/glow/blob/master/core/src/main/scala/org/hadoop_bam/util/GlowBGZFOutputStream.scala
 * https://github.com/projectglow/glow/blob/master/core/src/main/scala/io/projectglow/sql/util/BGZFCodec.scala
 *
 */
class BGZFOutputStream(val sink: OutputStream) : CompressionOutputStream(sink) {
    protected val nullFile: File? = null
    protected var htsjdkBGZF = BlockCompressedOutputStream(sink, nullFile)

    override fun write(i: Int) {
        htsjdkBGZF.write(i)
    }

    override fun write(dat: ByteArray, ofs: Int, len: Int) {
        htsjdkBGZF.write(dat, ofs, len)
    }

    override fun finish() {
        htsjdkBGZF.flush()
        // NO htsjdkBGZF.finish() to omit EOF marker
    }

    override fun resetState() {
        htsjdkBGZF.flush()
        htsjdkBGZF = BlockCompressedOutputStream(sink, nullFile)
    }

    override fun close() {
        htsjdkBGZF.flush()
        // NO htsjdkBGZF.close() to omit EOF marker
        sink.close()
    }
}
