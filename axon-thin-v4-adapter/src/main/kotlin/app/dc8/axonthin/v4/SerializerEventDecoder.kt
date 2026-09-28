package app.dc8.axonthin.v4

import app.dc8.axonthin.api.EventDecoder
import app.dc8.axonthin.api.Payload
import org.axonframework.messaging.MetaData
import org.axonframework.serialization.Serializer
import org.axonframework.serialization.SimpleSerializedObject
import org.axonframework.serialization.SimpleSerializedType
import org.axonframework.serialization.UnknownSerializedType

/**
 * [EventDecoder] on Axon's [Serializer] — the engine's event serializer, so objects come out exactly as the engine
 * reads them. A class missing from the classpath is [Payload.Unknown] (Axon's serializers report it as
 * UnknownSerializedType instead of failing); anything else that goes wrong is [Payload.Failed].
 */
class SerializerEventDecoder(private val serializer: Serializer) : EventDecoder {

    private val metaDataType = SimpleSerializedType(MetaData::class.java.name, null)

    override fun metaData(data: ByteArray): Map<String, Any?> =
        serializer.deserialize<ByteArray, MetaData>(SimpleSerializedObject(data, ByteArray::class.java, metaDataType)).toMap()

    override fun payload(payloadType: String, payloadRevision: String?, data: ByteArray): Payload {
        val type = SimpleSerializedType(payloadType, payloadRevision)
        return try {
            if (serializer.classForType(type) == UnknownSerializedType::class.java) Payload.Unknown(payloadType, payloadRevision)
            else Payload.Decoded(serializer.deserialize<ByteArray, Any>(SimpleSerializedObject(data, ByteArray::class.java, type)))
        } catch (e: Exception) {
            Payload.Failed(e)
        } catch (e: LinkageError) {
            Payload.Failed(e)
        }
    }
}
