package app.dc8.example.task.contract

import app.dc8.example.task.api.TaskStatus
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import org.axonframework.serialization.Serializer
import org.axonframework.serialization.json.JacksonSerializer
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary

/**
 * The application-defined serializer as real projects declare it: a general `Serializer` bean qualified "serializer",
 * built on a *copy* of the primary ObjectMapper with extra modules. Axon 4 uses it for events (it replaces Axon's
 * general serializer); thin must pick the same bean, or stored bytes would differ.
 *
 * The extra module writes [TaskStatus] in lowercase, so the stored JSON shows which serializer wrote it.
 */
@Configuration(proxyBeanMethods = false)
class ApplicationSerializerFixture {

    /** `@Primary`: Axon's own auto-configuration injects `Serializer` by type, so Axon 4 does not start without it. */
    @Bean
    @Primary
    @Qualifier("serializer")
    fun axonJacksonSerializer(objectMapper: ObjectMapper): Serializer {
        val axonObjectMapper = objectMapper.copy().registerModule(
            SimpleModule("lowercase-task-status")
                .addSerializer(TaskStatus::class.java, object : JsonSerializer<TaskStatus>() {
                    override fun serialize(value: TaskStatus, gen: JsonGenerator, serializers: SerializerProvider) =
                        gen.writeString(value.name.lowercase())
                })
                .addDeserializer(TaskStatus::class.java, object : JsonDeserializer<TaskStatus>() {
                    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): TaskStatus =
                        TaskStatus.valueOf(p.text.uppercase())
                }),
        )
        return JacksonSerializer.builder().objectMapper(axonObjectMapper).build()
    }
}
