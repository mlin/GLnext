/**
 * Compression codec wrapping BGZFOutputStream.
 *
 * Based on GLOW and Hadoop-BAM (Apache and MIT licenses, respectively):
 * https://github.com/projectglow/glow/blob/master/core/src/main/scala/io/projectglow/sql/util/BGZFCodec.scala
 * https://github.com/HadoopGenomics/Hadoop-BAM/blob/master/src/main/java/org/seqdoop/hadoop_bam/util/BGZFCodec.java
 *
 */

import org.apache.hadoop.io.compress.CompressionOutputStream
import org.apache.hadoop.io.compress.Compressor
import org.apache.hadoop.io.compress.GzipCodec
import java.io.OutputStream

class BGZFCodec : GzipCodec() {
    override fun createOutputStream(sink: OutputStream): CompressionOutputStream {
        return BGZFOutputStream(sink)
    }

    override fun createOutputStream(sink: OutputStream, moot: Compressor?): CompressionOutputStream {
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
