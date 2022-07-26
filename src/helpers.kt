import java.io.*
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

@Suppress("UNCHECKED_CAST")
fun <T : Serializable> deserializeFromByteArray(buf: ByteArray): T {
    ByteArrayInputStream(buf).use {
        ObjectInputStream(it).use {
            return it.readObject() as T
        }
    }
}

fun Serializable.serializeToByteArray(): ByteArray {
    ByteArrayOutputStream().use {
        ObjectOutputStream(it).use {
            it.writeObject(this)
        }
        return it.toByteArray()
    }
}

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
 * Open file InputStream, gunzipping if applicable
 */
fun openMaybeGzFile(filename: String, fs: FileSystem? = null): InputStream {
    val fs2 = if (fs != null) { fs } else { getFileSystem(filename) }
    var instream: InputStream = fs2.open(Path(filename))
    // TODO: decide based on magic bytes instead of filename
    if (filename.endsWith(".gz") || filename.endsWith(".bgz")) {
        instream = java.util.zip.GZIPInputStream(instream)
    }
    return instream
}
