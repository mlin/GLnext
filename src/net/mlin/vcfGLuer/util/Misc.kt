package net.mlin.vcfGLuer.util
import java.util.PriorityQueue
import kotlin.math.min
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.expressions.Aggregator

fun Int.pretty(): String = java.text.NumberFormat.getIntegerInstance().format(this)
fun Long.pretty(): String = java.text.NumberFormat.getIntegerInstance().format(this)

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
 * Make an evenly-partitioned RDD from the given list of N items for parallel processing, avoiding
 * any tendency for a lot of empty partitions when N <= spark.default.parallelism.
 */
fun <T> JavaSparkContext.parallelizeEvenly(items: List<T>): JavaRDD<T> {
    val N = items.size
    if (N >= 2 * defaultParallelism()) {
        return parallelize(items)
    }
    val prdd = parallelizePairs(items.mapIndexed { i, it -> scala.Tuple2<Int, T>(i, it) })
    val M = min(N, defaultParallelism())
    return prdd.partitionBy(object : org.apache.spark.Partitioner() {
        override fun numPartitions(): Int {
            return M
        }
        override fun getPartition(k: Any): Int {
            val ki = k as Int
            return ki % M
        }
    }).values()
}

/**
 * SparkSQL Aggregator for the Nth largest int in a [group] column
 */
class NthLargestInt(private val n: Int) : Aggregator<Int, IntPriorityQueue, Int?>() {
    init {
        require(n > 0)
    }

    override fun zero(): IntPriorityQueue = IntPriorityQueue()

    override fun reduce(buffer: IntPriorityQueue, input: Int): IntPriorityQueue {
        buffer.add(input)
        while (buffer.size > n) {
            buffer.poll()
        }
        return buffer
    }

    override fun merge(
        buffer1: IntPriorityQueue,
        buffer2: IntPriorityQueue
    ): IntPriorityQueue {
        buffer2.forEach { this.reduce(buffer1, it) }
        return buffer1
    }

    override fun finish(reduction: IntPriorityQueue): Int? =
        if (reduction.size >= n) {
            reduction.peek()
        } else {
            null
        }

    override fun bufferEncoder(): Encoder<IntPriorityQueue> =
        Encoders.kryo(IntPriorityQueue::class.java)

    override fun outputEncoder(): Encoder<Int?> = Encoders.INT()
}

// this is needed just to overcome issues with type erasure in bufferEncoder() above
class IntPriorityQueue() : PriorityQueue<Int>()
