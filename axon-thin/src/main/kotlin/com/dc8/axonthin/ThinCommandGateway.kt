package com.dc8.axonthin

import com.dc8.axonthin.api.BulkCommandGateway
import org.axonframework.commandhandling.CommandCallback
import org.axonframework.commandhandling.CommandMessage
import org.axonframework.commandhandling.CommandResultMessage
import org.axonframework.commandhandling.GenericCommandMessage
import org.axonframework.commandhandling.GenericCommandResultMessage
import org.axonframework.commandhandling.NoHandlerForCommandException
import org.axonframework.commandhandling.gateway.CommandGateway
import org.axonframework.common.Registration
import org.axonframework.messaging.MessageDispatchInterceptor
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

/**
 * Axon [CommandGateway] that handles commands synchronously on the calling thread (like SimpleCommandBus),
 * each inside a Spring transaction together with the event handlers of the events it publishes.
 *
 * `send(...)` variants return already-completed futures; timeouts are accepted but have nothing to wait for.
 */
class ThinCommandGateway internal constructor(
    private val registry: ThinHandlerRegistry,
    private val eventGateway: ThinEventGateway,
    private val transactions: ThinTransactions,
) : CommandGateway, BulkCommandGateway {

    private val dispatchInterceptors = CopyOnWriteArrayList<MessageDispatchInterceptor<in CommandMessage<*>>>()

    override fun <C : Any?, R : Any?> send(command: C & Any, callback: CommandCallback<in C, in R>) {
        val message = prepare(command)
        @Suppress("UNCHECKED_CAST")
        val typed = message as CommandMessage<C>
        val result: CommandResultMessage<R> = try {
            @Suppress("UNCHECKED_CAST")
            GenericCommandResultMessage(dispatch(message) as R)
        } catch (e: RuntimeException) {
            GenericCommandResultMessage.asCommandResultMessage(e)
        }
        callback.onResult(typed, result)
    }

    @Suppress("UNCHECKED_CAST")
    override fun <R : Any?> sendAndWait(command: Any): R = dispatch(prepare(command)) as R

    override fun <R : Any?> sendAndWait(command: Any, timeout: Long, unit: TimeUnit): R = sendAndWait(command)

    override fun <R : Any?> send(command: Any): CompletableFuture<R> =
        try {
            CompletableFuture.completedFuture(sendAndWait(command))
        } catch (e: RuntimeException) {
            CompletableFuture.failedFuture(e)
        }

    /** One transaction for the whole batch; each command still gets its own unit of work (events flushed per command). */
    override fun sendAllAndWait(commands: List<Any>): List<Any?> {
        val messages = commands.map(::prepare)
        return transactions.inTransaction { messages.map(::dispatch) }
    }

    override fun registerDispatchInterceptor(
        dispatchInterceptor: MessageDispatchInterceptor<in CommandMessage<*>>,
    ): Registration {
        dispatchInterceptors += dispatchInterceptor
        return Registration { dispatchInterceptors.remove(dispatchInterceptor) }
    }

    private fun prepare(command: Any): CommandMessage<*> {
        var message: CommandMessage<*> = GenericCommandMessage.asCommandMessage<Any>(command)
        ThinUnitOfWork.currentOrNull()?.currentMessage?.let { message = message.andMetaData(correlationData(it)) }
        @Suppress("UNCHECKED_CAST")
        return dispatchInterceptors.fold(message) { msg, interceptor ->
            (interceptor as MessageDispatchInterceptor<CommandMessage<*>>).handle(msg)
        }
    }

    private fun dispatch(command: CommandMessage<*>): Any? {
        val handler = registry.commandHandler(command.commandName)
            ?: throw NoHandlerForCommandException(
                "No handler was subscribed for command [${command.commandName}].",
            )
        return ThinUnitOfWork.joinOrStart { uow, isRoot ->
            transactions.inTransaction {
                val result = uow.handling(command) { handler.invoke(command) }
                if (isRoot) eventGateway.flush(uow)
                result
            }
        }
    }
}
