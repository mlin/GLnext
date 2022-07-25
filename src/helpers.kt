import java.io.*

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
