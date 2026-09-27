package com.dc8.example.task

import com.dc8.example.task.api.AddCommentCommand
import com.dc8.example.task.api.AssignTaskCommand
import com.dc8.example.task.api.CommentAddedEvent
import com.dc8.example.task.api.CreateTaskCommand
import com.dc8.example.task.api.ImportTaskCommand
import com.dc8.example.task.api.RenameTaskCommand
import com.dc8.example.task.api.TaskAssignedEvent
import com.dc8.example.task.api.TaskCreatedEvent
import com.dc8.example.task.api.TaskRenamedEvent
import com.dc8.example.task.contract.StoredEvents
import jakarta.persistence.EntityManager
import org.assertj.core.api.Assertions.assertThat
import org.axonframework.commandhandling.GenericCommandMessage
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.common.jpa.SimpleEntityManagerProvider
import org.axonframework.eventhandling.DomainEventMessage
import org.axonframework.eventhandling.GenericDomainEventMessage
import org.axonframework.eventsourcing.eventstore.jpa.JpaEventStorageEngine
import org.axonframework.serialization.Serializer
import org.axonframework.spring.messaging.unitofwork.SpringTransactionManager
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.util.UUID
import kotlin.streams.toList

/**
 * axon-thin and Axon 4's own JpaEventStorageEngine on the same database, as during a migration:
 * the Axon tables are created by Hibernate from Axon's entities (renamed by META-INF/axon-orm.xml, like the real
 * project), and both engines read and write them.
 *
 * Projections are not the subject here (events appended by Axon's storage engine are never dispatched), so handler
 * errors are logged instead of propagated.
 */
@SpringBootTest(
    properties = [
        "spring.jpa.mapping-resources=META-INF/axon-orm.xml",
        "axon.thin.event-handler-error-mode=log",
    ],
)
class AxonStorageInteropTest {

    @Autowired lateinit var commandGateway: CommandGateway
    @Autowired lateinit var entityManager: EntityManager
    @Autowired lateinit var transactionManager: PlatformTransactionManager
    @Autowired @Qualifier("eventSerializer") lateinit var serializer: Serializer
    @Autowired lateinit var jdbc: JdbcTemplate

    private val axon by lazy {
        JpaEventStorageEngine.builder()
            .entityManagerProvider(SimpleEntityManagerProvider(entityManager))
            .transactionManager(SpringTransactionManager(transactionManager))
            .eventSerializer(serializer)
            .snapshotSerializer(serializer)
            .build()
    }
    private val stored by lazy { StoredEvents(jdbc, "axon_domain_event_entry") }

    @BeforeEach
    fun clean() = stored.deleteAll()

    private fun id() = UUID.randomUUID().toString()

    private fun axonAppend(vararg events: DomainEventMessage<*>) {
        TransactionTemplate(transactionManager).executeWithoutResult { axon.appendEvents(events.toList()) }
    }

    private fun axonRead(aggregateId: String): List<DomainEventMessage<*>> =
        TransactionTemplate(transactionManager).execute { axon.readEvents(aggregateId).asStream().toList() }!!

    @Test
    fun `Axon reads the aggregate stream written by thin`() {
        val taskId = id()
        val create = GenericCommandMessage.asCommandMessage<CreateTaskCommand>(CreateTaskCommand(taskId, "thin", "d"))
            .andMetaData(mapOf("userId" to "ann"))
        commandGateway.sendAndWait<String>(create)
        commandGateway.sendAndWait<Any>(AssignTaskCommand(taskId, "bob"))
        commandGateway.sendAndWait<String>(AddCommentCommand(taskId, "c1", "bob", "hi"))

        val events = axonRead(taskId)

        assertThat(events.map { it.payload }).containsExactly(
            TaskCreatedEvent(taskId, "thin", "d", "ann"),
            TaskAssignedEvent(taskId, "bob"),
            CommentAddedEvent(taskId, "c1", "bob", "hi"),
        )
        assertThat(events.map { it.sequenceNumber }).containsExactly(0L, 1L, 2L)
        assertThat(events).allSatisfy {
            assertThat(it.type).isEqualTo("Task")
            assertThat(it.aggregateIdentifier).isEqualTo(taskId)
        }
        assertThat(events.first().metaData).containsEntry("correlationId", create.identifier)
    }

    @Test
    fun `thin continues an aggregate stream written by Axon`() {
        val taskId = id()
        axonAppend(
            GenericDomainEventMessage("Task", taskId, 0, TaskCreatedEvent(taskId, "from axon", null, "legacy")),
            GenericDomainEventMessage("Task", taskId, 1, CommentAddedEvent(taskId, "c1", "legacy", "old comment")),
        )

        commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "renamed by thin"))
        // CREATE_IF_MISSING finds the Axon-written aggregate instead of creating a new one
        commandGateway.sendAndWait<Any>(ImportTaskCommand(taskId, "imported"))

        val events = axonRead(taskId)
        assertThat(events.map { it.sequenceNumber }).containsExactly(0L, 1L, 2L, 3L)
        assertThat(events.drop(2).map { it.payload }).containsExactly(
            TaskRenamedEvent(taskId, "renamed by thin"),
            TaskRenamedEvent(taskId, "imported"),
        )
    }

    @Test
    fun `global index allocation from both engines never collides`() {
        val ids = List(120) { id() }

        // alternate engines so both keep crossing 50-value sequence blocks
        ids.forEachIndexed { i, taskId ->
            if (i % 2 == 0) axonAppend(GenericDomainEventMessage("Task", taskId, 0, TaskCreatedEvent(taskId, "axon $i", null, null)))
            else commandGateway.sendAndWait<String>(CreateTaskCommand(taskId, "thin $i"))
        }

        val rows = stored.all()
        assertThat(rows).hasSize(120)
        assertThat(rows.map { it.globalIndex }).doesNotHaveDuplicates().allSatisfy { assertThat(it).isPositive() }
    }
}
