package com.dc8.example.task.contract

import com.dc8.axonthin.api.BulkCommandGateway
import com.dc8.example.task.TaskService
import com.dc8.example.task.api.AddCommentCommand
import com.dc8.example.task.api.AssignTaskCommand
import com.dc8.example.task.api.ChangeTaskStatusCommand
import com.dc8.example.task.api.CreateTaskCommand
import com.dc8.example.task.api.DeleteCommentCommand
import com.dc8.example.task.api.RenameTaskCommand
import com.dc8.example.task.api.TaskClosedException
import com.dc8.example.task.api.TaskNotFoundException
import com.dc8.example.task.api.TaskRenamedEvent
import com.dc8.example.task.api.TaskStatus
import com.dc8.example.task.domain.CommentRepository
import com.dc8.example.task.domain.TaskRepository
import com.dc8.example.task.projection.TaskActivityRepository
import com.dc8.example.task.projection.TaskSummaryRepository
import com.dc8.example.task.query.TaskQueryService
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.axonframework.commandhandling.CommandCallback
import org.axonframework.commandhandling.CommandMessage
import org.axonframework.commandhandling.CommandResultMessage
import org.axonframework.commandhandling.GenericCommandMessage
import org.axonframework.commandhandling.NoHandlerForCommandException
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.eventhandling.gateway.EventGateway
import org.axonframework.messaging.MetaData
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Behavioural contract shared by every engine. Each app module extends this with a `@SpringBootTest`
 * subclass, so the very same assertions run against real Axon 4 and against axon-thin.
 *
 * Engine configuration assumed by the contract: subscribing event processors, no event store,
 * event handler errors propagate (roll back the command).
 */
abstract class TaskContractTest {

    @Autowired lateinit var commandGateway: CommandGateway
    @Autowired lateinit var eventGateway: EventGateway
    @Autowired lateinit var bulk: BulkCommandGateway
    @Autowired lateinit var taskService: TaskService
    @Autowired lateinit var queries: TaskQueryService
    @Autowired lateinit var tasks: TaskRepository
    @Autowired lateinit var comments: CommentRepository
    @Autowired lateinit var summaries: TaskSummaryRepository
    @Autowired lateinit var activities: TaskActivityRepository

    @BeforeEach
    fun cleanDatabase() {
        listOf(activities, summaries, comments, tasks).forEach { it.deleteAllInBatch() }
    }

    private fun id() = UUID.randomUUID().toString()

    private fun createTask(title: String = "Write docs"): String =
        commandGateway.sendAndWait(CreateTaskCommand(id(), title))

    // ---- command gateway --------------------------------------------------------------------------------------------

    @Test
    fun `sendAndWait returns the handler result and projections are updated before it returns`() {
        val taskId = id()

        val result: String = commandGateway.sendAndWait(CreateTaskCommand(taskId, "  Write docs  ", "all of them"))

        assertThat(result).isEqualTo(taskId)
        assertThat(tasks.findById(taskId)).hasValueSatisfying { assertThat(it.title).isEqualTo("Write docs") }
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

        assertThatThrownBy { future.get(5, TimeUnit.SECONDS) }.cause().isInstanceOf(TaskNotFoundException::class.java)
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
    fun `metadata and spring beans are resolved as handler parameters`() {
        val taskId = id()
        val command = GenericCommandMessage.asCommandMessage<CreateTaskCommand>(CreateTaskCommand(taskId, "Meta"))
            .andMetaData(MetaData.with("userId", "alice"))

        commandGateway.sendAndWait<String>(command)

        val task = tasks.findById(taskId).orElseThrow()
        assertThat(task.createdBy).isEqualTo("alice")
        assertThat(task.createdAt).isNotNull()
    }

    @Test
    fun `sendAndWait with metadata overload`() {
        val taskId = id()

        commandGateway.sendAndWait<String>(CreateTaskCommand(taskId, "Meta"), MetaData.with("userId", "bob"))

        assertThat(tasks.findById(taskId).orElseThrow().createdBy).isEqualTo("bob")
    }

    @Test
    fun `runtime exception from handler is rethrown as-is and nothing is persisted`() {
        val taskId = id()

        assertThatThrownBy { commandGateway.sendAndWait<Any>(CreateTaskCommand(taskId, "  ")) }
            .isExactlyInstanceOf(IllegalArgumentException::class.java)
            .hasMessage("Task title must not be blank")
        assertThat(tasks.existsById(taskId)).isFalse()
        assertThat(queries.summary(taskId)).isNull()
    }

    @Test
    fun `business rule violation keeps previous state`() {
        val taskId = createTask()
        commandGateway.sendAndWait<Any>(ChangeTaskStatusCommand(taskId, TaskStatus.DONE))

        assertThatThrownBy { commandGateway.sendAndWait<Any>(AddCommentCommand(id(), taskId, "bob", "late")) }
            .isInstanceOf(TaskClosedException::class.java)
        assertThat(queries.comments(taskId)).isEmpty()
        assertThat(queries.summary(taskId)!!.commentCount).isZero()
    }

    @Test
    fun `unknown command fails with NoHandlerForCommandException`() {
        assertThatThrownBy { commandGateway.sendAndWait<Any>(UnknownCommand("x")) }
            .isInstanceOf(NoHandlerForCommandException::class.java)
    }

    // ---- event handling ---------------------------------------------------------------------------------------------

    @Test
    fun `comment lifecycle keeps the summary counter in sync`() {
        val taskId = createTask()
        val first = id()
        commandGateway.sendAndWait<String>(AddCommentCommand(first, taskId, "ann", "one"))
        commandGateway.sendAndWait<String>(AddCommentCommand(id(), taskId, "ann", "two"))
        commandGateway.sendAndWait<Any>(DeleteCommentCommand(first))

        assertThat(queries.summary(taskId)!!.commentCount).isEqualTo(1)
        assertThat(queries.comments(taskId).map { it.text }).containsExactly("two")
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
        assertThat(log.map { it.eventId }).doesNotHaveDuplicates().doesNotContainNull()
        assertThat(log).allSatisfy { assertThat(it.occurredAt).isNotNull() }
        assertThat(log.first().correlationId).isEqualTo(create.identifier)
    }

    @Test
    fun `event handler failure rolls back the command (same transaction)`() {
        val taskId = id()

        assertThatThrownBy { commandGateway.sendAndWait<Any>(CreateTaskCommand(taskId, FailingProjection.POISON_TITLE)) }
            .satisfies({ assertThat(rootCause(it)).hasMessage(FailingProjection.FAILURE_MESSAGE) })
        assertThat(tasks.existsById(taskId)).isFalse()
        assertThat(queries.summary(taskId)).isNull()
        assertThat(queries.activity(taskId)).isEmpty()
    }

    @Test
    fun `events published outside a command are handled immediately`() {
        val taskId = createTask("Before")

        eventGateway.publish(TaskRenamedEvent(taskId, "After"))

        assertThat(queries.summary(taskId)!!.title).isEqualTo("After")
    }

    // ---- bulk -------------------------------------------------------------------------------------------------------

    @Test
    fun `sendAllAndWait returns results in command order`() {
        val ids = List(50) { id() }

        val results = bulk.sendAllAndWait(ids.mapIndexed { i, taskId -> CreateTaskCommand(taskId, "Task $i") })

        assertThat(results).containsExactlyElementsOf(ids)
        assertThat(queries.countTasks()).isEqualTo(50)
    }

    @Test
    fun `sendAllAndWait is all-or-nothing`() {
        val ok1 = id()
        val ok2 = id()

        assertThatThrownBy {
            bulk.sendAllAndWait(
                listOf(CreateTaskCommand(ok1, "one"), CreateTaskCommand(ok2, "two"), CreateTaskCommand(id(), " ")),
            )
        }.isExactlyInstanceOf(IllegalArgumentException::class.java)

        assertThat(tasks.count()).isZero()
        assertThat(summaries.count()).isZero()
        assertThat(activities.count()).isZero()
    }

    @Test
    fun `sendAllAndWait lets later commands see earlier effects`() {
        val taskId = id()
        val commentId = id()

        val results = bulk.sendAllAndWait(
            listOf(
                CreateTaskCommand(taskId, "Bulk"),
                AddCommentCommand(commentId, taskId, "dan", "first!"),
                AssignTaskCommand(taskId, "dan"),
                ChangeTaskStatusCommand(taskId, TaskStatus.IN_PROGRESS),
            ),
        )

        assertThat(results).containsExactly(taskId, commentId, null, TaskStatus.TODO)
        val summary = queries.summary(taskId)!!
        assertThat(summary.commentCount).isEqualTo(1)
        assertThat(summary.assignee).isEqualTo("dan")
        assertThat(summary.status).isEqualTo(TaskStatus.IN_PROGRESS)
    }

    @Test
    fun `sendAllAndWait accepts command messages with metadata`() {
        val taskId = id()

        bulk.sendAllAndWait(
            listOf(GenericCommandMessage.asCommandMessage<Any>(CreateTaskCommand(taskId, "m")).andMetaData(mapOf("userId" to "eve"))),
        )

        assertThat(tasks.findById(taskId).orElseThrow().createdBy).isEqualTo("eve")
    }

    @Test
    fun `sendAllAndWait joins the caller transaction`() {
        val a = createTask("a")
        val b = createTask("b")

        assertThatThrownBy { taskService.closeAll(listOf(a, b, "missing")) }
            .isInstanceOf(TaskNotFoundException::class.java)
        assertThat(queries.byStatus(TaskStatus.DONE)).isEmpty()

        val previous = taskService.closeAll(listOf(a, b))
        assertThat(previous).containsExactlyInAnyOrderEntriesOf(mapOf(a to TaskStatus.TODO, b to TaskStatus.TODO))
        assertThat(queries.byStatus(TaskStatus.DONE).map { it.taskId }).containsExactlyInAnyOrder(a, b)
    }

    @Test
    fun `bulk import of many tasks`() {
        val ids = taskService.importTasks(List(1_000) { "Imported $it" })

        assertThat(ids).hasSize(1_000).doesNotHaveDuplicates()
        assertThat(queries.countTasks()).isEqualTo(1_000)
        assertThat(activities.count()).isEqualTo(1_000)
    }

    // ---- helpers ----------------------------------------------------------------------------------------------------

    data class UnknownCommand(val id: String)

    private fun rootCause(e: Throwable): Throwable = generateSequence(e) { it.cause }.last()

    private fun await(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (!condition()) {
            check(System.nanoTime() < deadline) { "condition not met within 5s" }
            Thread.onSpinWait()
        }
    }
}
