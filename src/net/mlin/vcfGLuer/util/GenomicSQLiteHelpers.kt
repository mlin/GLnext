package net.mlin.vcfGLuer.util
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import org.apache.commons.dbcp2.BasicDataSource

/**
 * Create new GenomicSQLite database file, configured for bulk loading
 */
fun createGenomicSQLiteForBulkLoad(filename: String, threads: Int = 2): Connection {
    val config = Properties()
    config.setProperty(
        "genomicsqlite.config_json",
        """{
            "threads": $threads,
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
            "immutable": true,
            "vfs_log": 2
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
