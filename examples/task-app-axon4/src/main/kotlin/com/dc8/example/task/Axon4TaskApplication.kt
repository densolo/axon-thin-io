package com.dc8.example.task

import org.axonframework.config.EventProcessingConfigurer
import org.axonframework.eventhandling.EventBus
import org.axonframework.eventhandling.PropagatingErrorHandler
import org.axonframework.eventhandling.SimpleEventBus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean

/**
 * The example model wired with the real Axon 4 engine, configured to the semantics axon-thin reproduces:
 * local SimpleCommandBus, SimpleEventBus (no event store), subscribing processors, propagating handler errors.
 */
@SpringBootApplication
class Axon4TaskApplication {

    @Bean
    fun eventBus(): EventBus = SimpleEventBus.builder().build()

    @Autowired
    fun configureEventProcessing(processing: EventProcessingConfigurer) {
        processing.usingSubscribingEventProcessors()
        processing.registerDefaultListenerInvocationErrorHandler { PropagatingErrorHandler.instance() }
    }
}

fun main(args: Array<String>) {
    runApplication<Axon4TaskApplication>(*args)
}
