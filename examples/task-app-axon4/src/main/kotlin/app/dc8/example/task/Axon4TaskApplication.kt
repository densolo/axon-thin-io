package app.dc8.example.task

import org.axonframework.config.EventProcessingConfigurer
import org.axonframework.eventhandling.PropagatingErrorHandler
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

/**
 * The example model wired with the real Axon 4 engine, configured to the semantics axon-thin reproduces:
 * local SimpleCommandBus, JPA event store (prefixed tables via META-INF/axon-orm.xml), Jackson serializer,
 * subscribing processors, propagating handler errors.
 */
@SpringBootApplication
class Axon4TaskApplication {

    @Autowired
    fun configureEventProcessing(processing: EventProcessingConfigurer) {
        processing.usingSubscribingEventProcessors()
        processing.registerDefaultListenerInvocationErrorHandler { PropagatingErrorHandler.instance() }
    }
}

fun main(args: Array<String>) {
    runApplication<Axon4TaskApplication>(*args)
}
