/**
 * CompressionOutputStream for writing out a Dataset<String> to BGZF shards. The shards omit the
 * BGZF EOF marker so that they can later be concatenated into one giant file.
 *
 * Based on GLOW's (Apache-licensed):
 * https://github.com/projectglow/glow/blob/master/core/src/main/scala/org/hadoop_bam/util/GlowBGZFOutputStream.scala
 * https://github.com/projectglow/glow/blob/master/core/src/main/scala/io/projectglow/sql/util/BGZFCodec.scala
 *
 */

import htsjdk.samtools.util.BlockCompressedOutputStream
import org.apache.hadoop.io.compress.CompressionOutputStream
import java.io.File
import java.io.OutputStream

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
