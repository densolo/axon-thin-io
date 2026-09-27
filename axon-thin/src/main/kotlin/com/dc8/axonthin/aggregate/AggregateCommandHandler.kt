package com.dc8.axonthin.aggregate

import com.dc8.axonthin.HandlerMethod
import com.dc8.axonthin.ThinUnitOfWork
import com.dc8.axonthin.eventstore.ThinEventStore
import org.axonframework.commandhandling.CommandMessage
import org.axonframework.eventsourcing.AggregateDeletedException
import org.axonframework.modelling.command.AggregateCreationPolicy
import org.axonframework.modelling.command.AggregateNotFoundException
import org.axonframework.modelling.command.TargetAggregateIdentifier
import org.springframework.core.annotation.AnnotatedElementUtils
import java.lang.reflect.AccessibleObject
import java.lang.reflect.Field
import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap

/**
 * Routes one command to an aggregate, mirroring Axon's AggregateAnnotationCommandHandler:
 * - constructor handler → new aggregate, result = aggregate identifier;
 * - NEVER (default) → load by `@TargetAggregateIdentifier`, result = method result;
 * - ALWAYS → new instance, result = method result, or the identifier for `void` handlers;
 * - CREATE_IF_MISSING → load, or new instance when absent; result = method result.
 */
internal class AggregateCommandHandler(
    private val model: AggregateModel,
    val commandName: String,
    private val handler: HandlerMethod,
    private val policy: AggregateCreationPolicy,
    private val isConstructor: Boolean,
) {
    fun handle(command: CommandMessage<*>, uow: ThinUnitOfWork, store: ThinEventStore): Any? = when {
        isConstructor -> {
            val aggregate = ThinAggregate(model, null)
            aggregate.attachRoot(aggregate.inScope { handler.construct(command) })
            aggregate.identifierValue
        }
        policy == AggregateCreationPolicy.ALWAYS -> createAndHandle(command, uow)
        policy == AggregateCreationPolicy.CREATE_IF_MISSING -> {
            val id = targetIdentifier(command, required = false)
            val existing = id?.let { load(it, uow, store, required = false) }
            if (existing != null) invoke(existing, command) else createAndHandle(command, uow, voidAsIdentifier = false)
        }
        else -> invoke(load(targetIdentifier(command, required = true)!!, uow, store, required = true)!!, command)
    }

    private fun createAndHandle(command: CommandMessage<*>, uow: ThinUnitOfWork, voidAsIdentifier: Boolean = true): Any? {
        val aggregate = ThinAggregate(model, model.newInstance())
        val result = invoke(aggregate, command)
        return if (voidAsIdentifier && handler.returnsVoid) aggregate.identifierValue else result
    }

    private fun invoke(aggregate: ThinAggregate, command: CommandMessage<*>): Any? =
        aggregate.inScope { handler.invokeOn(aggregate.root!!, command) }

    /** Identity map first (per command, or per batch), then the event store. */
    private fun load(id: String, uow: ThinUnitOfWork, store: ThinEventStore, required: Boolean): ThinAggregate? {
        val key = model.typeName to id
        val aggregate = uow.aggregates[key] ?: run {
            val events = store.readEvents(id)
            if (events.isEmpty()) {
                if (required) throw AggregateNotFoundException(id, "The aggregate was not found in the event store")
                return null
            }
            ThinAggregate(model, model.newInstance()).also {
                it.initializeState(events)
                uow.aggregates[key] = it
            }
        }
        if (aggregate.deleted) throw AggregateDeletedException(id)
        return aggregate
    }

    private fun targetIdentifier(command: CommandMessage<*>, required: Boolean): String? {
        val value = targetAccessor(command.payloadType)?.let { accessor ->
            when (accessor) {
                is Field -> accessor.get(command.payload)
                is Method -> accessor.invoke(command.payload)
                else -> null
            }
        }
        if (value == null && required) {
            throw IllegalArgumentException(
                "Invalid command. It does not identify the target aggregate. Make sure at least one of the fields " +
                    "or methods in the [${command.payloadType.simpleName}] class contains the " +
                    "@TargetAggregateIdentifier annotation and that it returns a non-null value.",
            )
        }
        return value?.toString()
    }

    override fun toString(): String = "$model $handler"

    companion object {
        private val NONE = Any()
        private val targetAccessors = ConcurrentHashMap<Class<*>, Any>()

        /** `@TargetAggregateIdentifier` field or no-arg method, searched through the class hierarchy. */
        private fun targetAccessor(type: Class<*>): AccessibleObject? = targetAccessors.computeIfAbsent(type) {
            var current: Class<*>? = it
            while (current != null && current != Any::class.java) {
                current.declaredFields.firstOrNull { f -> AnnotatedElementUtils.hasAnnotation(f, TargetAggregateIdentifier::class.java) }
                    ?.let { f -> f.trySetAccessible(); return@computeIfAbsent f }
                current.declaredMethods.firstOrNull { m ->
                    m.parameterCount == 0 && AnnotatedElementUtils.hasAnnotation(m, TargetAggregateIdentifier::class.java)
                }?.let { m -> m.trySetAccessible(); return@computeIfAbsent m }
                current = current.superclass
            }
            NONE
        } as? AccessibleObject
    }
}
