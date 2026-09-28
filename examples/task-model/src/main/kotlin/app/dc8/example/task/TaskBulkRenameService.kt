package app.dc8.example.task

import app.dc8.axonthin.api.BulkCommandGateway
import app.dc8.example.task.api.RenameTaskCommand
import app.dc8.example.task.api.TaskCreatedEvent
import app.dc8.example.task.api.TasksBulkRenamedEvent
import org.axonframework.eventhandling.GenericDomainEventMessage
import org.axonframework.eventsourcing.eventstore.EventStore
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

/**
 * The pre-thin bulk pattern, kept to show it runs unchanged on both engines:
 * flagged per-aggregate events for the aggregates, plus one bulk event for projections — and creating an aggregate by
 * publishing its first event directly, bypassing the aggregate. On thin, batch event handlers make both unnecessary.
 */
@Service
class TaskBulkRenameService(
    private val bulk: BulkCommandGateway,
    private val eventStore: EventStore,
) {
    @Transactional
    fun renameAll(titlesById: Map<String, String>) {
        bulk.sendAllAndWait<Any?>(titlesById.map { (id, title) -> RenameTaskCommand(id, title, bulk = true) })
        eventStore.publish(
            GenericDomainEventMessage(
                BULK_TYPE, "$BULK_TYPE-${UUID.randomUUID()}", 0,
                TasksBulkRenamedEvent(titlesById.map { (id, title) -> TasksBulkRenamedEvent.Rename(id, title) }),
            ),
        )
    }

    /** Creates a Task by publishing its creation event directly — no aggregate involved. */
    fun createDirectly(taskId: String, title: String) {
        eventStore.publish(GenericDomainEventMessage("Task", taskId, 0, TaskCreatedEvent(taskId, title, null, null)))
    }

    companion object {
        const val BULK_TYPE = "BulkTaskRename"
    }
}
