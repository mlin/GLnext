package net.mlin.vcfGLuer.util
import java.io.Serializable

fun Int.pretty(): String = java.text.NumberFormat.getIntegerInstance().format(this)
fun Long.pretty(): String = java.text.NumberFormat.getIntegerInstance().format(this)

@Suppress("UNCHECKED_CAST")
fun <T : Serializable> deserializeFromByteArray(buf: ByteArray): T {
    java.io.ByteArrayInputStream(buf).use {
        java.io.ObjectInputStream(it).use {
            return it.readObject() as T
        }
    }
}

fun Serializable.serializeToByteArray(): ByteArray {
    java.io.ByteArrayOutputStream().use {
        java.io.ObjectOutputStream(it).use {
            it.writeObject(this)
        }
        return it.toByteArray()
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
