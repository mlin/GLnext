package net.mlin.vcfGLuer.util

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
