package net.mlin.vcfGLuer.util
import java.io.File
import java.io.InputStream
import java.io.Reader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
 * Get the Hadoop FileSystem object for path (either hdfs: or a local filename)
 */
fun getFileSystem(path: String): FileSystem {
    val normPath = if (path.startsWith("hdfs:") || path.startsWith("file://")) {
        path
    } else {
        "file://" + path
    }
    return FileSystem.get(
        java.net.URI(normPath),
        org.apache.spark.deploy.SparkHadoopUtil.get().conf()
    )
}

/**
 * Recursively list a [HDFS] directory, as a Sequence
 */
fun FileSystem.listSequence(
    path: String,
    recursive: Boolean
): Sequence<org.apache.hadoop.fs.LocatedFileStatus> {
    val it = getFileSystem(path).listFiles(Path(path), recursive)
    return sequence {
        while (it.hasNext()) {
            yield(it.next())
        }
    }
}

/**
 * Open reader for file, gunzipping if applicable
 */
fun fileReaderDetectGz(
    filename: String,
    fs: FileSystem? = null,
    bufferSize: Int = 65536
): Reader {
    val fs2 = fs ?: getFileSystem(filename)
    var instream: InputStream = fs2.open(Path(filename))
    // TODO: decide based on magic bytes instead of filename
    if (filename.endsWith(".gz") || filename.endsWith(".bgz")) {
        instream = java.util.zip.GZIPInputStream(instream, bufferSize)
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
 * Concatenate several local files into dest
 */
fun concatFiles(src: List<String>, dest: String, chunkSize: Int = 1048576) {
    File(dest).outputStream().use { destOut ->
        val buf = ByteArray(chunkSize)
        src.forEach { srcFilename ->
            val srcFile = File(srcFilename)
            check(srcFile.isFile())
            srcFile.inputStream().use { srcIn ->
                var n: Int
                while (srcIn.read(buf).also { n = it } >= 0) {
                    destOut.write(buf, 0, n)
                }
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