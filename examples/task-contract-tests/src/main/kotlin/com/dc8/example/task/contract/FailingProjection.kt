package com.dc8.example.task.contract

import com.dc8.example.task.api.TaskCreatedEvent
import org.axonframework.eventhandling.EventHandler
import org.springframework.stereotype.Component

/**
 * Test-only projection (on the classpath of the app modules' tests only): fails for a poison title,
 * to prove projections share the command's transaction.
 */
@Component
class FailingProjection {

    @EventHandler
    fun on(event: TaskCreatedEvent) {
        if (event.title == POISON_TITLE) throw IllegalStateException(FAILURE_MESSAGE)
    }

    companion object {
        const val POISON_TITLE = "poison-projection"
        const val FAILURE_MESSAGE = "projection failed on purpose"
    }
}
