import java.io.*
import java.sql.*
import java.util.Properties
import org.apache.commons.dbcp2.BasicDataSource
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

/**
 * A file is on HDFS, and may also be copied on the local filesystem. If not then create the copy.
 * Not inherently concurrency-safe!
 */
fun ensureLocalCopy(hdfsPath: String, localFilename: String) {
    val localFile = File(localFilename)
    if (localFile.createNewFile() || localFile.length() == 0L) {
        val tempFile = File.createTempFile(localFile.getName() + ".", ".tmp")
        getFileSystem(hdfsPath).copyToLocalFile(Path(hdfsPath), Path(tempFile.absolutePath))
        tempFile.renameTo(localFile)
    }
}

/**
 * As python contextlib.ExitStack
 */
class ExitStack : AutoCloseable {
    private val resources: ArrayDeque<AutoCloseable> = ArrayDeque()

    fun <T : AutoCloseable> add(resource: T): T {
        resources.add(resource)
        return resource
    }

    override fun close() {
        var firstExc: Throwable? = null
        var resource: AutoCloseable? = resources.removeLastOrNull()
        while (resource != null) {
            try {
                resource.close()
            } catch (exc: Throwable) {
                if (firstExc == null) {
                    firstExc = exc
                }
            }
            resource = resources.removeLastOrNull()
        }
        if (firstExc != null) {
            throw firstExc
        }
    }
}

/**
 * Create new GenomicSQLite database file, configured for bulk loading
 */
fun createGenomicSQLiteForBulkLoad(filename: String, threads: Int = 2): Connection {
    val config = Properties()
    config.setProperty(
        "genomicsqlite.config_json",
        """{
            "threads": $threads,
            "zstd_level": 3,
            "inner_page_KiB": 64,
            "outer_page_KiB": 2,
            "unsafe_load": true,
            "page_cache_MiB": 256
        }"""
    )

    val dbc = DriverManager.getConnection("jdbc:genomicsqlite:" + filename, config)
    dbc.setAutoCommit(false)
    return dbc
}

/**
 * Formulate configuration JSON for read-only GenomicSQLite database connection
 */
fun getGenomicSQLiteReadOnlyConfigJSON(threads: Int = 2): String {
    return """{
            "threads": $threads,
            "page_cache_MiB": 256,
            "immutable": true
        }"""
}

/**
 * Open existing GenomicSQLite database file for read-only ops
 */
fun openGenomicSQLiteReadOnly(filename: String, threads: Int = 2): Connection {
    val config = org.sqlite.SQLiteConfig()
    config.setReadOnly(true)
    val props = config.toProperties()
    props.setProperty(
        "genomicsqlite.config_json",
        getGenomicSQLiteReadOnlyConfigJSON(threads = threads)
    )
    return DriverManager.getConnection("jdbc:genomicsqlite:" + filename, props)
}

/**
 * Pool of read-only connections to one GenomicSQLite database file
 */
class GenomicSQLiteReadOnlyPool {
    companion object {
        @Volatile
        private var INSTANCE: BasicDataSource? = null
        private var dbFilename: String? = null

        @Synchronized
        fun get(dbFilename: String): BasicDataSource {
            if (INSTANCE == null) {
                INSTANCE = cons(dbFilename)
                this.dbFilename = dbFilename
            } else {
                require(dbFilename == this.dbFilename)
            }
            return INSTANCE!!
        }

        private fun cons(dbFilename: String): BasicDataSource {
            val ans = BasicDataSource()
            ans.setDriverClassName("net.mlin.genomicsqlite.JdbcDriver")
            ans.setUrl("jdbc:genomicsqlite:" + dbFilename)
            ans.addConnectionProperty(
                "genomicsqlite.config_json",
                getGenomicSQLiteReadOnlyConfigJSON()
            )
            val ncpu = Runtime.getRuntime().availableProcessors()
            ans.setMaxIdle(ncpu)
            ans.setMaxTotal(ncpu)
            ans.setPoolPreparedStatements(true)
            return ans
        }
    }
}
