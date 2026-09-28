package app.dc8.example.task.contract

import app.dc8.axonthin.api.BulkCommandGateway
import app.dc8.example.task.TaskBulkRenameService
import app.dc8.example.task.TaskService
import app.dc8.example.task.api.AddCommentCommand
import app.dc8.example.task.api.AssignTaskCommand
import app.dc8.example.task.api.ChangeTaskStatusCommand
import app.dc8.example.task.api.CommentNotFoundException
import app.dc8.example.task.api.CreateTaskCommand
import app.dc8.example.task.api.CreateTasksFromTemplateCommand
import app.dc8.example.task.api.DeleteCommentCommand
import app.dc8.example.task.api.DeleteTaskCommand
import app.dc8.example.task.api.EditCommentCommand
import app.dc8.example.task.api.ImportTaskCommand
import app.dc8.example.task.api.RenameTaskCommand
import app.dc8.example.task.api.TaskClosedException
import app.dc8.example.task.api.TaskRenamedEvent
import app.dc8.example.task.api.TaskStatus
import app.dc8.example.task.api.TasksImportedEvent
import app.dc8.example.task.projection.CommentViewRepository
import app.dc8.example.task.projection.TaskActivityRepository
import app.dc8.example.task.projection.TaskSummaryRepository
import app.dc8.example.task.query.TaskQueryService
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.axonframework.commandhandling.CommandCallback
import org.axonframework.commandhandling.CommandMessage
import org.axonframework.commandhandling.CommandResultMessage
import org.axonframework.commandhandling.GenericCommandMessage
import org.axonframework.commandhandling.NoHandlerForCommandException
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.eventhandling.GenericDomainEventMessage
import org.axonframework.eventhandling.gateway.EventGateway
import org.axonframework.eventsourcing.eventstore.EventStore
import org.axonframework.eventsourcing.AggregateDeletedException
import org.axonframework.messaging.MetaData
import org.axonframework.modelling.command.AggregateNotFoundException
import org.axonframework.modelling.command.AggregateStreamCreationException
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.TestPropertySource
import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Behavioural contract shared by every engine. Each app module extends this with a `@SpringBootTest`
 * subclass, so the very same assertions run against real Axon 4 and against axon-thin.
 *
 * Engine configuration assumed by the contract: event-sourced aggregates on Axon's `domain_event_entry` layout
 * (tables prefixed `axon_`), Jackson serializer, subscribing event processors, event handler errors propagate.
 */
@TestPropertySource(properties = ["task.snapshot-threshold=5"])
abstract class TaskContractTest {

    @Autowired lateinit var commandGateway: CommandGateway
    @Autowired lateinit var eventGateway: EventGateway
    @Autowired lateinit var bulk: BulkCommandGateway
    @Autowired lateinit var taskService: TaskService
    @Autowired lateinit var queries: TaskQueryService
    @Autowired lateinit var summaries: TaskSummaryRepository
    @Autowired lateinit var comments: CommentViewRepository
    @Autowired lateinit var activities: TaskActivityRepository
    @Autowired lateinit var jdbc: JdbcTemplate
    @Autowired lateinit var chunkPositions: ChunkPositionRecorder
    @Autowired lateinit var eventStore: EventStore
    @Autowired lateinit var bulkRename: TaskBulkRenameService

    protected open val eventTable = "axon_domain_event_entry"
    protected val stored by lazy { StoredEvents(jdbc, eventTable) }

    @BeforeEach
    fun cleanDatabase() {
        listOf(activities, summaries, comments).forEach { it.deleteAllInBatch() }
        stored.deleteAll()
    }

    private fun id() = UUID.randomUUID().toString()

    private fun createTask(title: String = "Write docs"): String =
        commandGateway.sendAndWait(CreateTaskCommand(id(), title))

    // ---- command gateway --------------------------------------------------------------------------------------------

    @Test
    fun `constructor command handler returns the aggregate id and projections are updated before it returns`() {
        val taskId = id()

        val result: String = commandGateway.sendAndWait(CreateTaskCommand(taskId, "  Write docs  ", "all of them"))

        assertThat(result).isEqualTo(taskId)
        val summary = queries.summary(taskId)!!
        assertThat(summary.title).isEqualTo("Write docs")
        assertThat(summary.status).isEqualTo(TaskStatus.TODO)
        assertThat(summary.commentCount).isZero()
    }

    @Test
    fun `sendAndWait returns null for Unit handlers`() {
        val taskId = createTask()

        val result: Any? = commandGateway.sendAndWait(RenameTaskCommand(taskId, "Renamed"))

        assertThat(result).isNull()
        assertThat(queries.summary(taskId)!!.title).isEqualTo("Renamed")
    }

    @Test
    fun `send returns a future with the handler result`() {
        val taskId = id()

        val future = commandGateway.send<String>(CreateTaskCommand(taskId, "Async"))

        assertThat(future.get(5, TimeUnit.SECONDS)).isEqualTo(taskId)
        assertThat(queries.summary(taskId)).isNotNull
    }

    @Test
    fun `send completes the future exceptionally with the original exception`() {
        val future = commandGateway.send<Any>(RenameTaskCommand(id(), "x"))

        assertThatThrownBy { future.get(5, TimeUnit.SECONDS) }.cause().isInstanceOf(AggregateNotFoundException::class.java)
    }

    @Test
    fun `send with callback reports result`() {
        val taskId = id()
        val result = AtomicReference<CommandResultMessage<*>>()

        commandGateway.send(
            CreateTaskCommand(taskId, "Callback"),
            CommandCallback<CreateTaskCommand, String> { _: CommandMessage<out CreateTaskCommand>, r: CommandResultMessage<out String> -> result.set(r) },
        )

        await { result.get() != null }
        assertThat(result.get().isExceptional).isFalse()
        assertThat(result.get().payload).isEqualTo(taskId)
    }

    @Test
    fun `metadata is resolved as handler parameter`() {
        val taskId = id()
        val command = GenericCommandMessage.asCommandMessage<CreateTaskCommand>(CreateTaskCommand(taskId, "Meta"))
            .andMetaData(MetaData.with("userId", "alice"))

        commandGateway.sendAndWait<String>(command)

        assertThat(queries.summary(taskId)!!.createdBy).isEqualTo("alice")
    }

    @Test
    fun `sendAndWait with metadata overload`() {
        val taskId = id()

        commandGateway.sendAndWait<String>(CreateTaskCommand(taskId, "Meta"), MetaData.with("userId", "bob"))

        assertThat(queries.summary(taskId)!!.createdBy).isEqualTo("bob")
    }

    @Test
    fun `runtime exception from handler is rethrown as-is and nothing is stored`() {
        val taskId = id()

        assertThatThrownBy { commandGateway.sendAndWait<Any>(CreateTaskCommand(taskId, "  ")) }
            .isExactlyInstanceOf(IllegalArgumentException::class.java)
            .hasMessage("Task title must not be blank")
        assertThat(stored.count()).isZero()
        assertThat(queries.summary(taskId)).isNull()
    }

    @Test
    fun `unknown command fails with NoHandlerForCommandException`() {
        assertThatThrownBy { commandGateway.sendAndWait<Any>(UnknownCommand("x")) }
            .isInstanceOf(NoHandlerForCommandException::class.java)
    }

    // ---- aggregates -------------------------------------------------------------------------------------------------

    @Test
    fun `command for a missing aggregate fails with AggregateNotFoundException`() {
        assertThatThrownBy { commandGateway.sendAndWait<Any>(RenameTaskCommand("missing", "x")) }
            .isExactlyInstanceOf(AggregateNotFoundException::class.java)
    }

    @Test
    fun `creating an existing aggregate fails with AggregateStreamCreationException`() {
        val taskId = createTask()

        assertThatThrownBy { commandGateway.sendAndWait<Any>(CreateTaskCommand(taskId, "again")) }
            .isInstanceOf(AggregateStreamCreationException::class.java)
        assertThat(stored.forAggregate(taskId)).hasSize(1)
    }

    @Test
    fun `state is rebuilt from stored events`() {
        val taskId = createTask()
        commandGateway.sendAndWait<String>(AddCommentCommand(taskId, "c1", "ann", "one"))
        commandGateway.sendAndWait<Any>(DeleteCommentCommand(taskId, "c1"))

        // the aggregate only knows c1 is gone because it replays CommentAdded + CommentDeleted
        assertThatThrownBy { commandGateway.sendAndWait<Any>(EditCommentCommand(taskId, "c1", "edited")) }
            .isInstanceOf(CommentNotFoundException::class.java)

        commandGateway.sendAndWait<Any>(ChangeTaskStatusCommand(taskId, TaskStatus.DONE))
        assertThatThrownBy { commandGateway.sendAndWait<Any>(AddCommentCommand(taskId, "c2", "bob", "late")) }
            .isInstanceOf(TaskClosedException::class.java)
        assertThat(queries.comments(taskId)).isEmpty()
    }

    @Test
    fun `markDeleted makes the aggregate unavailable`() {
        val taskId = createTask()

        commandGateway.sendAndWait<Any>(DeleteTaskCommand(taskId))

        assertThat(queries.summary(taskId)).isNull()
        assertThatThrownBy { commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "zombie")) }
            .isInstanceOf(AggregateDeletedException::class.java)
    }

    @Test
    fun `CREATE_IF_MISSING creates, then updates the same aggregate`() {
        val taskId = id()

        val first: Any? = commandGateway.sendAndWait(ImportTaskCommand(taskId, "v1"))
        val second: Any? = commandGateway.sendAndWait(ImportTaskCommand(taskId, "v2"))

        assertThat(first).isEqualTo(taskId)
        assertThat(second).isEqualTo(taskId)
        assertThat(stored.forAggregate(taskId).map { it.payloadType.substringAfterLast('.') })
            .containsExactly("TaskCreatedEvent", "TaskRenamedEvent")
        assertThat(queries.summary(taskId)!!.title).isEqualTo("v2")
    }

    @Test
    fun `external handler dispatches nested commands in the same unit of work`() {
        val template = GenericCommandMessage.asCommandMessage<CreateTasksFromTemplateCommand>(
            CreateTasksFromTemplateCommand("Sprint", listOf("plan", "build", "ship")),
        )

        val ids: List<String> = commandGateway.sendAndWait(template)

        assertThat(ids).hasSize(3)
        assertThat(ids.map { queries.summary(it)!!.title }).containsExactly("Sprint plan", "Sprint build", "Sprint ship")
        // traceId flows command -> nested commands -> their events
        assertThat(stored.all()).hasSize(4).allSatisfy { assertThat(it.metaData["traceId"]).isEqualTo(template.identifier) }
    }

    @Test
    fun `AggregateLifecycle corner cases match Axon`() {
        val probeId = id()

        commandGateway.sendAndWait<Any>(StartProbeCommand(probeId))
        val observed: String = commandGateway.sendAndWait(PokeProbeCommand(probeId))

        val rows = stored.forAggregate(probeId)
        assertThat(rows.map { it.payloadType.substringAfterLast('.') to it.payload - "probeId" }).containsExactly(
            "ProbeStartedEvent" to emptyMap<String, Any?>(),
            // andThenApply (queued while the constructor ran) goes before the apply nested in the sourcing handler
            "ProbeStepEvent" to mapOf("step" to "and-then-apply", "version" to 0),
            // getVersion() inside the sourcing handler already reflects the event being handled
            "ProbeStepEvent" to mapOf("step" to "from-sourcing-handler", "version" to 0),
            "ProbePokedEvent" to mapOf("live" to true),
        )
        assertThat(rows.map { it.sequenceNumber }).containsExactly(0L, 1L, 2L, 3L)
        assertThat(observed).isEqualTo(PROBE_OBSERVED)
    }

    // ---- snapshots (EventCountSnapshotTriggerDefinition, threshold 5) -----------------------------------------------

    private fun createTaskWithRenames(renames: Int): String {
        val taskId = createTask("r0")
        (1..renames).forEach { commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "r$it")) }
        return taskId
    }

    @Test
    fun `snapshot is stored once the trigger threshold is reached`() {
        val taskId = createTaskWithRenames(3)
        val fifth = GenericCommandMessage.asCommandMessage<RenameTaskCommand>(RenameTaskCommand(taskId, "r4"))
        commandGateway.sendAndWait<Any>(fifth) // 5 events: seq 0..4

        val snapshots = stored.snapshots(taskId)

        assertThat(snapshots.map { it.sequenceNumber }).containsExactly(SNAPSHOT_AFTER_5_EVENTS)
        val snapshot = snapshots.single()
        assertThat(snapshot.type).isEqualTo("Task")
        assertThat(snapshot.payloadType).isEqualTo("app.dc8.example.task.domain.Task")
        assertThat(snapshot.payload).containsEntry("taskId", taskId).containsEntry("title", "r4")
        // built in the unit of work of the command that crossed the threshold
        assertThat(snapshot.metaData).isEqualTo(mapOf("correlationId" to fifth.identifier, "traceId" to fifth.identifier))
    }

    @Test
    fun `newer snapshot replaces the older one`() {
        val taskId = createTaskWithRenames(11) // 12 events: seq 0..11

        assertThat(stored.snapshots(taskId).map { it.sequenceNumber }).containsExactly(SNAPSHOT_AFTER_12_EVENTS)
    }

    @Test
    fun `aggregate is restored from the snapshot`() {
        val taskId = createTaskWithRenames(4)
        commandGateway.sendAndWait<String>(AddCommentCommand(taskId, "c0", "ann", "before snapshot"))
        val snapshotSeq = stored.snapshots(taskId).maxOf { it.sequenceNumber }
        stored.deleteEventsUpTo(taskId, snapshotSeq) // only the snapshot knows the state now

        commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "r4")) // same title: no event if state was restored
        commandGateway.sendAndWait<Any>(DeleteCommentCommand(taskId, "c0")) // c0 known only via the snapshot/stream

        assertThat(stored.forAggregate(taskId).map { it.payloadType.substringAfterLast('.') })
            .endsWith("CommentDeletedEvent")
            .doesNotContain("TaskRenamedEvent")
    }

    @Test
    fun `bulk commands on one aggregate leave a usable snapshot`() {
        val taskId = id()

        bulk.sendAllAndWait(listOf(CreateTaskCommand(taskId, "b0")) + (1..11).map { RenameTaskCommand(taskId, "b$it") })

        assertThat(stored.snapshots(taskId)).hasSize(1)
        stored.deleteEventsUpTo(taskId, stored.snapshots(taskId).single().sequenceNumber)
        commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "b12"))
        assertThat(queries.summary(taskId)!!.title).isEqualTo("b12")
    }

    // ---- chunk context (validation during bulk) --------------------------------------------------------------------

    @Test
    fun `validation sees changes made earlier in the same chunk`() {
        val title = "unique-${id()}"

        assertThatThrownBy {
            bulk.sendAllAndWait(listOf(CreateTaskCommand(id(), title), ReserveTitleCommand(title)))
        }.isInstanceOf(TitleTakenException::class.java)
        assertThat(stored.count()).isZero()
    }

    @Test
    fun `validation sees committed state and lets unrelated titles pass`() {
        val title = "unique-${id()}"
        createTask(title)

        assertThatThrownBy { bulk.sendAllAndWait(listOf(ReserveTitleCommand(title))) }
            .isInstanceOf(TitleTakenException::class.java)
        bulk.sendAllAndWait(listOf(ReserveTitleCommand("free-${id()}"), CreateTaskCommand(id(), "other-${id()}")))
    }

    @Test
    fun `chunk context reports the chunk's commands and the current position`() {
        chunkPositions.seen.clear()

        bulk.sendAllAndWait(
            listOf(CreateTaskCommand(id(), "x"), RecordChunkPositionCommand("bulk"), CreateTaskCommand(id(), "y")),
        )
        commandGateway.sendAndWait<Any>(RecordChunkPositionCommand("single"))

        assertThat(chunkPositions.seen).containsExactly(Triple("bulk", 1, 3), Triple("single", 0, 1))
    }

    // ---- direct EventStore use (pre-thin patterns that must keep working) ---------------------------------------------

    @Test
    fun `bulk pattern - flagged single events plus a bulk event published to the EventStore`() {
        val ids = List(3) { createTask("before $it") }

        bulkRename.renameAll(ids.associateWith { "bulk $it" })
        commandGateway.sendAndWait<Any>(RenameTaskCommand(ids[0], "single after bulk"))

        assertThat(ids.map { queries.summary(it)!!.title })
            .containsExactly("single after bulk", "bulk ${ids[1]}", "bulk ${ids[2]}")
        ids.forEach { id ->
            assertThat(stored.forAggregate(id)[1].payload).containsEntry("title", "bulk $id").containsEntry("bulk", true)
        }
        val envelope = stored.all().single { it.type == TaskBulkRenameService.BULK_TYPE }
        assertThat(envelope.aggregateIdentifier).startsWith("${TaskBulkRenameService.BULK_TYPE}-")
        assertThat(envelope.sequenceNumber).isZero()
        @Suppress("UNCHECKED_CAST")
        assertThat((envelope.payload["renames"] as List<Map<String, Any>>).map { it["taskId"] }).containsExactlyElementsOf(ids)
    }

    @Test
    fun `an aggregate created by publishing its first event directly can be used by commands`() {
        val taskId = id()

        bulkRename.createDirectly(taskId, "direct")
        commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "renamed by command"))

        assertThat(queries.summary(taskId)!!.title).isEqualTo("renamed by command")
        assertThat(stored.forAggregate(taskId).map { it.sequenceNumber to it.type }).containsExactly(0L to "Task", 1L to "Task")
    }

    @Test
    fun `EventStore readEvents returns the stream, with the latest snapshot first`() {
        val taskId = createTask("r0")
        (1..2).forEach { commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "r$it")) }

        assertThat(eventStore.readEvents(taskId).asStream().map { it.sequenceNumber to it.payloadType.simpleName }.toList())
            .containsExactly(0L to "TaskCreatedEvent", 1L to "TaskRenamedEvent", 2L to "TaskRenamedEvent")

        (3..5).forEach { commandGateway.sendAndWait<Any>(RenameTaskCommand(taskId, "r$it")) } // snapshot at seq 4
        val withSnapshot = eventStore.readEvents(taskId).asStream().toList()
        assertThat(withSnapshot.map { it.sequenceNumber }).containsExactly(4L, 5L)
        assertThat(withSnapshot.first().payloadType.simpleName).isEqualTo("Task") // the snapshot: the aggregate itself
        assertThat(eventStore.readEvents(taskId, 3).asStream().map { it.sequenceNumber }.toList()).containsExactly(3L, 4L, 5L)
        assertThat(eventStore.lastSequenceNumberFor(taskId)).contains(5L)
    }

    @Test
    fun `publishing an event with a sequence number that is taken fails`() {
        val taskId = createTask("taken")

        assertThatThrownBy {
            eventStore.publish(GenericDomainEventMessage("Task", taskId, 0, TaskRenamedEvent(taskId, "dup")))
        }.isInstanceOf(AggregateStreamCreationException::class.java)
        assertThat(stored.forAggregate(taskId)).hasSize(1)
    }

    // ---- storage format (what the other engine / other services will read) -----------------------------------------

    @Test
    fun `aggregate events are stored in Axon's domain_event_entry layout`() {
        val taskId = id()
        val create = GenericCommandMessage.asCommandMessage<CreateTaskCommand>(CreateTaskCommand(taskId, "Stored", "d"))
            .andMetaData(mapOf("userId" to "zoe"))
        commandGateway.sendAndWait<String>(create)
        commandGateway.sendAndWait<Any>(AssignTaskCommand(taskId, "zoe"))

        val rows = stored.forAggregate(taskId)

        assertThat(rows).hasSize(2)
        val (created, assigned) = rows
        assertThat(rows.map { it.sequenceNumber }).containsExactly(0L, 1L)
        assertThat(rows).allSatisfy {
            assertThat(it.type).isEqualTo("Task")
            assertThat(it.aggregateIdentifier).isEqualTo(taskId)
            assertThat(Instant.parse(it.timeStamp)).isBefore(Instant.now().plusSeconds(1))
            assertThat(it.timeStamp).matches("""\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z""")
        }
        assertThat(created.globalIndex).isLessThan(assigned.globalIndex)
        assertThat(created.payloadType).isEqualTo("app.dc8.example.task.api.TaskCreatedEvent")
        assertThat(created.payloadRevision).isEqualTo("2")
        assertThat(created.payload).isEqualTo(
            mapOf("taskId" to taskId, "title" to "Stored", "description" to "d", "createdBy" to "zoe"),
        )
        assertThat(created.metaData).containsEntry("correlationId", create.identifier)
            .containsEntry("traceId", create.identifier)
            .doesNotContainKey("userId") // command metadata is not copied, only correlation data
        assertThat(assigned.payloadType).isEqualTo("app.dc8.example.task.api.TaskAssignedEvent")
        assertThat(assigned.payloadRevision).isNull()
        assertThat(assigned.payload).isEqualTo(mapOf("taskId" to taskId, "assignee" to "zoe"))
        assertThat(queries.activity(taskId).map { it.eventId to it.sequenceNumber })
            .containsExactly(created.eventIdentifier to 0L, assigned.eventIdentifier to 1L)
    }

    @Test
    fun `events are written with the application's serializer bean`() {
        val taskId = createTask()

        commandGateway.sendAndWait<Any>(ChangeTaskStatusCommand(taskId, TaskStatus.IN_PROGRESS))
        commandGateway.sendAndWait<Any>(ChangeTaskStatusCommand(taskId, TaskStatus.DONE)) // reload: reads it back

        // ApplicationSerializerFixture writes TaskStatus in lowercase: the stored JSON proves which serializer was used
        assertThat(stored.forAggregate(taskId).drop(1).map { it.payload["from"] to it.payload["to"] })
            .containsExactly("todo" to "in_progress", "in_progress" to "done")
        assertThat(queries.summary(taskId)!!.status).isEqualTo(TaskStatus.DONE)
    }

    @Test
    fun `events published outside a command are handled and stored as non-aggregate events`() {
        val taskId = createTask("Before")

        eventGateway.publish(TaskRenamedEvent(taskId, "After"), TasksImportedEvent(7, "manual"))

        assertThat(queries.summary(taskId)!!.title).isEqualTo("After")
        val nonAggregate = stored.all().filter { it.type == null }
        assertThat(nonAggregate).hasSize(2).allSatisfy {
            assertThat(it.aggregateIdentifier).isEqualTo(it.eventIdentifier)
            assertThat(it.sequenceNumber).isZero()
        }
        assertThat(nonAggregate.last().payload).isEqualTo(mapOf("count" to 7, "source" to "manual"))
    }

    // ---- event handling ---------------------------------------------------------------------------------------------

    @Test
    fun `comment lifecycle keeps the read models in sync`() {
        val taskId = createTask()
        commandGateway.sendAndWait<String>(AddCommentCommand(taskId, "c1", "ann", "one"))
        commandGateway.sendAndWait<String>(AddCommentCommand(taskId, "c2", "ann", "two"))
        commandGateway.sendAndWait<Any>(EditCommentCommand(taskId, "c2", "two!"))
        commandGateway.sendAndWait<Any>(DeleteCommentCommand(taskId, "c1"))

        assertThat(queries.summary(taskId)!!.commentCount).isEqualTo(1)
        assertThat(queries.comments(taskId).map { it.text }).containsExactly("two!")
    }

    @Test
    fun `supertype handler sees every event with id, timestamp and correlation to the command`() {
        val taskId = id()
        val create = GenericCommandMessage.asCommandMessage<CreateTaskCommand>(CreateTaskCommand(taskId, "Audit"))
        commandGateway.sendAndWait<String>(create)
        commandGateway.sendAndWait<Any>(AssignTaskCommand(taskId, "carol"))
        commandGateway.sendAndWait<Any>(ChangeTaskStatusCommand(taskId, TaskStatus.IN_PROGRESS))

        val log = queries.activity(taskId)
        assertThat(log.map { it.type })
            .containsExactly("TaskCreatedEvent", "TaskAssignedEvent", "TaskStatusChangedEvent")
        assertThat(log.map { it.sequenceNumber }).containsExactly(0L, 1L, 2L)
        assertThat(log.map { it.eventId }).doesNotHaveDuplicates().doesNotContainNull()
        assertThat(log.first().correlationId).isEqualTo(create.identifier)
    }

    @Test
    fun `event handler failure rolls back the command and its events (same transaction)`() {
        val taskId = id()

        assertThatThrownBy { commandGateway.sendAndWait<Any>(CreateTaskCommand(taskId, FailingProjection.POISON_TITLE)) }
            .satisfies({ assertThat(rootCause(it)).hasMessage(FailingProjection.FAILURE_MESSAGE) })
        assertThat(stored.count()).isZero()
        assertThat(queries.summary(taskId)).isNull()
        assertThat(queries.activity(taskId)).isEmpty()
    }

    // ---- bulk -------------------------------------------------------------------------------------------------------

    @Test
    fun `sendAllAndWait returns results in command order`() {
        val ids = List(50) { id() }

        val results = bulk.sendAllAndWait(ids.mapIndexed { i, taskId -> CreateTaskCommand(taskId, "Task $i") })

        assertThat(results).containsExactlyElementsOf(ids)
        assertThat(queries.countTasks()).isEqualTo(50)
        assertThat(stored.count()).isEqualTo(50)
    }

    @Test
    fun `sendAllAndWait is all-or-nothing`() {
        assertThatThrownBy {
            bulk.sendAllAndWait(
                listOf(CreateTaskCommand(id(), "one"), CreateTaskCommand(id(), "two"), CreateTaskCommand(id(), " ")),
            )
        }.isExactlyInstanceOf(IllegalArgumentException::class.java)

        assertThat(stored.count()).isZero()
        assertThat(summaries.count()).isZero()
        assertThat(activities.count()).isZero()
    }

    @Test
    fun `sendAllAndWait rolls back on a duplicate aggregate inside the batch`() {
        val taskId = id()

        assertThatThrownBy {
            bulk.sendAllAndWait(listOf(CreateTaskCommand(taskId, "one"), CreateTaskCommand(taskId, "dup")))
        }.isInstanceOf(AggregateStreamCreationException::class.java)

        assertThat(stored.count()).isZero()
        assertThat(summaries.count()).isZero()
    }

    @Test
    fun `sendAllAndWait lets later commands see earlier effects on the same aggregate`() {
        val taskId = id()

        val results = bulk.sendAllAndWait(
            listOf(
                CreateTaskCommand(taskId, "Bulk"),
                AddCommentCommand(taskId, "c1", "dan", "first!"),
                AssignTaskCommand(taskId, "dan"),
                ChangeTaskStatusCommand(taskId, TaskStatus.IN_PROGRESS),
                ChangeTaskStatusCommand(taskId, TaskStatus.DONE),
            ),
        )

        assertThat(results).containsExactly(taskId, "c1", null, TaskStatus.TODO, TaskStatus.IN_PROGRESS)
        val summary = queries.summary(taskId)!!
        assertThat(summary.commentCount).isEqualTo(1)
        assertThat(summary.assignee).isEqualTo("dan")
        assertThat(summary.status).isEqualTo(TaskStatus.DONE)
        assertThat(stored.forAggregate(taskId).map { it.sequenceNumber }).containsExactly(0L, 1L, 2L, 3L, 4L)
    }

    @Test
    fun `sendAllAndWait accepts command messages with metadata`() {
        val taskId = id()

        bulk.sendAllAndWait(
            listOf(GenericCommandMessage.asCommandMessage<Any>(CreateTaskCommand(taskId, "m")).andMetaData(mapOf("userId" to "eve"))),
        )

        assertThat(queries.summary(taskId)!!.createdBy).isEqualTo("eve")
    }

    @Test
    fun `sendAllAndWait joins the caller transaction`() {
        val a = createTask("a")
        val b = createTask("b")

        assertThatThrownBy { taskService.closeAll(listOf(a, b, "missing")) }
            .isInstanceOf(AggregateNotFoundException::class.java)
        assertThat(queries.byStatus(TaskStatus.DONE)).isEmpty()
        assertThat(stored.count()).isEqualTo(2)

        val previous = taskService.closeAll(listOf(a, b))
        assertThat(previous).containsExactlyInAnyOrderEntriesOf(mapOf(a to TaskStatus.TODO, b to TaskStatus.TODO))
        assertThat(queries.byStatus(TaskStatus.DONE).map { it.taskId }).containsExactlyInAnyOrder(a, b)
    }

    @Test
    fun `bulk upsert creates missing and updates existing aggregates`() {
        val existing = createTask("old title")
        val fresh = id()

        val ids = taskService.upsertTasks(linkedMapOf(existing to "new title", fresh to "brand new"))

        assertThat(ids).containsExactly(existing, fresh)
        assertThat(queries.summary(existing)!!.title).isEqualTo("new title")
        assertThat(queries.summary(fresh)!!.title).isEqualTo("brand new")
    }

    @Test
    fun `bulk import of many tasks`() {
        val ids = taskService.importTasks(List(1_000) { "Imported $it" })

        assertThat(ids).hasSize(1_000).doesNotHaveDuplicates()
        assertThat(queries.countTasks()).isEqualTo(1_000)
        assertThat(stored.count()).isEqualTo(1_000)
        assertThat(stored.all().map { it.globalIndex }).doesNotHaveDuplicates()
    }

    // ---- helpers ----------------------------------------------------------------------------------------------------

    data class UnknownCommand(val id: String)

    companion object {
        /** PostgreSQL database for `-Ppostgres`; no-op on H2. */
        @JvmStatic
        @DynamicPropertySource
        fun database(registry: DynamicPropertyRegistry) = PostgresSupport.register(registry, "contract")

        // observed on Axon 4.13 (the reference); axon-thin must produce the same
        // 3 events replayed while not live; @AggregateVersion is only read (state-stored), never written for event sourcing
        const val PROBE_OBSERVED = "replayed=3 before=null after=null"
        // the snapshot message itself counts towards the next threshold: 5 → seq 4, then 1 + 4 events → seq 8
        const val SNAPSHOT_AFTER_5_EVENTS = 4L
        const val SNAPSHOT_AFTER_12_EVENTS = 8L
    }

    private fun rootCause(e: Throwable): Throwable = generateSequence(e) { it.cause }.last()

    private fun await(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (!condition()) {
            check(System.nanoTime() < deadline) { "condition not met within 5s" }
            Thread.onSpinWait()
        }
    }
}
