package com.dc8.axonthin

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.axonframework.commandhandling.CommandExecutionException
import org.axonframework.commandhandling.CommandHandler
import org.axonframework.commandhandling.CommandMessage
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.eventhandling.EventHandler
import org.axonframework.eventhandling.EventMessage
import org.axonframework.eventhandling.gateway.EventGateway
import org.axonframework.messaging.MessageDispatchInterceptor
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.core.annotation.Order
import java.util.function.BiFunction

/** Engine behaviour not covered by the shared contract suite (no database, no transaction manager). */
class ThinEngineTest {

    private val runner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(ThinAxonAutoConfiguration::class.java))

    data class Ping(val value: String)
    data class Pong(val value: String)
    data class Nested(val value: String)
    data class CheckedFailure(val value: String)

    class Recorder {
        val seen = mutableListOf<String>()
    }

    class Handlers(private val events: EventGateway, private val commands: CommandGateway) {
        @CommandHandler
        fun handle(ping: Ping): String {
            events.publish(Pong(ping.value))
            commands.sendAndWait<Any>(Nested(ping.value))
            return "pong:${ping.value}"
        }

        @CommandHandler
        fun handle(nested: Nested, recorder: Recorder) {
            recorder.seen += "nested-command:${nested.value}"
            events.publish(Pong("nested-${nested.value}"))
        }

        @CommandHandler
        fun handle(@Suppress("UNUSED_PARAMETER") command: CheckedFailure) {
            throw java.io.IOException("disk full")
        }
    }

    @Order(1)
    class FirstListener(private val recorder: Recorder) {
        @EventHandler
        fun on(pong: Pong, message: EventMessage<*>) {
            recorder.seen += "first:${pong.value}:${message.metaData["correlationId"] != null}"
            if (pong.value == "boom") throw IllegalStateException("listener failed")
        }
    }

    @Order(2)
    class SecondListener(private val recorder: Recorder) {
        @EventHandler
        fun on(event: Any) {
            recorder.seen += "second:${(event as? Pong)?.value}"
        }
    }

    private fun withHandlers() = runner
        .withBean(Recorder::class.java)
        .withBean(Handlers::class.java)
        .withBean(FirstListener::class.java)
        .withBean(SecondListener::class.java)

    @Test
    fun `events of nested commands are dispatched after the root handler, in publication order`() {
        withHandlers().run { ctx ->
            val result: String = ctx.getBean(CommandGateway::class.java).sendAndWait(Ping("a"))

            assertThat(result).isEqualTo("pong:a")
            assertThat(ctx.getBean(Recorder::class.java).seen).containsExactly(
                "nested-command:a",
                "first:a:true", "second:a",
                "first:nested-a:true", "second:nested-a",
            )
        }
    }

    @Test
    fun `LOG mode (Axon default) keeps going after a failing event handler`() {
        withHandlers().run { ctx ->
            ctx.getBean(EventGateway::class.java).publish(Pong("boom"))

            assertThat(ctx.getBean(Recorder::class.java).seen).containsExactly("first:boom:false", "second:boom")
        }
    }

    @Test
    fun `PROPAGATE mode rethrows the handler exception`() {
        withHandlers().withPropertyValues("axon.thin.event-handler-error-mode=propagate").run { ctx ->
            assertThatThrownBy { ctx.getBean(EventGateway::class.java).publish(Pong("boom")) }
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessage("listener failed")
            assertThat(ctx.getBean(Recorder::class.java).seen).containsExactly("first:boom:false")
        }
    }

    @Test
    fun `checked exceptions are wrapped in CommandExecutionException`() {
        withHandlers().run { ctx ->
            assertThatThrownBy { ctx.getBean(CommandGateway::class.java).sendAndWait<Any>(CheckedFailure("x")) }
                .isInstanceOf(CommandExecutionException::class.java)
                .hasCauseInstanceOf(java.io.IOException::class.java)
        }
    }

    @Test
    fun `dispatch interceptors can enrich commands`() {
        withHandlers().run { ctx ->
            val gateway = ctx.getBean(CommandGateway::class.java)
            val registration = gateway.registerDispatchInterceptor(
                MessageDispatchInterceptor<CommandMessage<*>> {
                    BiFunction { _, message -> message.andMetaData(mapOf("tenant" to "t1")) }
                },
            )
            val seen = mutableListOf<Any?>()
            gateway.registerDispatchInterceptor(
                MessageDispatchInterceptor<CommandMessage<*>> {
                    BiFunction { _, message -> message.also { seen += it.metaData["tenant"] } }
                },
            )

            gateway.sendAndWait<String>(Ping("x"))
            registration.cancel()
            gateway.sendAndWait<String>(Ping("y"))

            // nested commands are dispatched through the gateway too
            assertThat(seen).containsExactly("t1", "t1", null, null)
        }
    }

    class DuplicateHandlers {
        @CommandHandler
        fun handle(@Suppress("UNUSED_PARAMETER") ping: Ping) = "dup"
    }

    @Test
    fun `duplicate command handlers fail startup`() {
        withHandlers().withBean(DuplicateHandlers::class.java).run { ctx ->
            assertThat(ctx).hasFailed()
            assertThat(ctx.startupFailure).hasMessageContaining("Duplicate command handler")
        }
    }

    @Test
    fun `backs off when another CommandGateway is present`() {
        runner.withBean(CommandGateway::class.java, { org.mockito.Mockito.mock(CommandGateway::class.java) }).run { ctx ->
            assertThat(ctx).hasSingleBean(CommandGateway::class.java)
            assertThat(ctx).doesNotHaveBean(ThinCommandGateway::class.java)
        }
    }
}
