package net.mlin.vcfGLuer.util
import java.io.BufferedReader
import java.io.File
import java.io.FilterInputStream
import java.io.IOException
import java.io.InputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
 * Get the Hadoop FileSystem object for path (either URI or a local filename)
 */
fun getFileSystem(path: String): FileSystem {
    val schemes = listOf("file:", "hdfs:", "s3:", "gs:", "http:", "https:")
    val normPath = if (schemes.any { path.startsWith(it) }) {
        path
    } else {
        "file://" + path
    }
    val conf = org.apache.spark.deploy.SparkHadoopUtil.get().conf()
    return FileSystem.get(java.net.URI(normPath), conf)
}

/**
 * Naive implementation of FileSystem.concat
 *
 * We use this because many of the concrete FileSystem implementations leave concat unsupported or
 * impose tricky restrictions (e.g. see hadoop's FSDirConcatOp.java)
 */
fun FileSystem.concatNaive(
    dst: Path,
    src: Array<Path>
) {
    var totalFileSize = 0L
    var bytesWritten = 0L
    this.create(dst, true).use { outfile ->
        src.forEach {
            totalFileSize += this.getFileStatus(it).getLen()
            this.open(it).use {
                bytesWritten += it.transferTo(outfile)
            }
        }
    }
    check(bytesWritten == totalFileSize)
}

/**
 * Opens a reader for a file, decompressing it on-the-fly if the file is gzipped.
 *
 * @param filename The path of the file to be read.
 * @param fs The `FileSystem` to be used. If `null`, the default file system for `filename` is used.
 * @param bufferSize The size of the buffer to be used by the `BufferedReader`. Default is 65536.
 * @param partial Whether to allow reading only part of the file. If `false`, an exception is thrown
 *                if the number of bytes read does not match the file's length. Default is `false`.
 * @return A `BufferedReader` for the file.
 *
 * @throws IOException If an I/O error occurs, or if `partial` is `false` and the number of bytes
 *                     read does not match the file's length.
 */
fun fileReaderDetectGz(
    filename: String,
    fs: FileSystem? = null,
    bufferSize: Int = 65536
): BufferedReader {
    val fs2 = fs ?: getFileSystem(filename)
    val path = Path(filename)
    var instream: InputStream = InputStreamWithExpectedLength(
        fs2.open(path),
        fs2.getFileStatus(path).getLen()
    )
    // TODO: decide based on magic bytes instead of filename
    if (filename.endsWith(".gz") || filename.endsWith(".bgz")) {
        instream = htsjdk.samtools.util.BlockCompressedInputStream(instream, true)
    }
    return instream.reader().buffered(bufferSize)
}

/**
 * Read a large file as a sequence of ByteArrays of given size (except the last one).
 */
fun readFileChunks(filename: String, chunkSize: Int): Sequence<ByteArray> {
    val file = File(filename)
    var todo = file.length()

    return sequence {
        file.inputStream().use {
            while (todo > 0) {
                val buf = ByteArray(chunkSize)
                var chunk = 0
                while (chunk.toLong() < todo && chunk < chunkSize) {
                    var n = it.read(buf, chunk, chunkSize - chunk)
                    check(n > 0)
                    chunk += n
                    check(chunk.toLong() <= todo)
                }

                if (chunk == chunkSize) {
                    yield(buf)
                } else {
                    yield(buf.copyOfRange(0, chunk))
                }

                check(chunk <= todo)
                todo -= chunk
            }
        }
    }
}

/**
 * Compute CRC32C checksum of the file
 */
fun fileCRC32C(filename: String): Long {
    val crc = java.util.zip.CRC32C()
    File(filename).inputStream().use { inp ->
        val buf = ByteArray(1048576)
        var n: Int
        while (inp.read(buf).also { n = it } >= 0) {
            crc.update(buf, 0, n)
        }
    }
    return crc.value
}

/**
 * InputStream wrapper that throws if the source stream indicates EOF without without having read
 * exactly expectedLength bytes. This safeguards against deficient error handling in specific
 * FileSystem implementations, which could result in apparently successful but incomplete reads.
 * It's OK for the client of the wrapped stream to close it before the end; the length check is
 * done only when the source stream indicates EOF.
 */
class InputStreamWithExpectedLength(source: InputStream, val expectedLength: Long) :
    FilterInputStream(source) {

    private var bytesRead: Long = 0

    @Throws(IOException::class)
    override fun read(): Int {
        val b = super.read()
        if (b != -1) {
            bytesRead++
        } else {
            checkExpectedLength()
        }
        return b
    }

    @Throws(IOException::class)
    override fun read(b: ByteArray): Int {
        val n = super.read(b)
        if (n > 0) {
            bytesRead += n.toLong()
        } else if (n < 0) {
            checkExpectedLength()
        } else {
            check(b.size == 0)
        }
        return n
    }

    @Throws(IOException::class)
    override fun read(b: ByteArray, off: Int, len: Int): Int {
        val n = super.read(b, off, len)
        if (n > 0) {
            bytesRead += n.toLong()
        } else if (n < 0) {
            checkExpectedLength()
        } else {
            check(len == 0)
        }
        return n
    }

    @Throws(IOException::class)
    private fun checkExpectedLength() {
        if (bytesRead != expectedLength) {
            throw IOException("Expected $expectedLength bytes, but read $bytesRead bytes")
        }
    }
}
