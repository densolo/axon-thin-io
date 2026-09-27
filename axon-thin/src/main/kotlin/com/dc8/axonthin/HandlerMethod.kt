package com.dc8.axonthin

import org.axonframework.commandhandling.CommandExecutionException
import org.axonframework.eventhandling.DomainEventMessage
import org.axonframework.eventhandling.EventMessage
import org.axonframework.eventhandling.SequenceNumber
import org.axonframework.eventhandling.Timestamp
import org.axonframework.messaging.Message
import org.axonframework.messaging.MetaData
import org.axonframework.messaging.annotation.AggregateType
import org.axonframework.messaging.annotation.MessageIdentifier
import org.axonframework.messaging.annotation.MetaDataValue
import org.axonframework.messaging.annotation.SourceId
import org.springframework.aop.support.AopUtils
import org.springframework.beans.factory.BeanFactory
import org.springframework.core.MethodParameter
import org.springframework.core.annotation.AnnotatedElementUtils
import java.lang.reflect.Constructor
import java.lang.reflect.Executable
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.time.Instant

/** Resolves one handler argument from the message being handled. */
internal fun interface ParameterResolver {
    fun resolve(message: Message<*>): Any?
}

/**
 * A `@CommandHandler` / `@EventHandler` / `@EventSourcingHandler` method or constructor.
 * Bound to a Spring singleton ([bean]) or invoked on an aggregate instance ([invokeOn]).
 *
 * Supported parameters (a subset of Axon 4's default resolvers):
 * - first parameter: the payload (or the full [Message] if it is declared as a Message type)
 * - any [Message] subtype, [MetaData], `@MetaDataValue`, `@MessageIdentifier`
 * - events: `@Timestamp`; domain events: `@SequenceNumber`, `@SourceId`, `@AggregateType`
 * - anything else: a Spring bean of that type (Axon's SpringBeanParameterResolverFactory)
 */
internal class HandlerMethod(
    val executable: Executable,
    val payloadType: Class<*>,
    private val resolvers: List<ParameterResolver>,
    private val bean: Any? = null,
) {
    private val target: Executable = when {
        bean != null && executable is Method -> AopUtils.selectInvocableMethod(executable, bean.javaClass)
        else -> executable
    }.also { it.trySetAccessible() }

    /** `true` when the Java method returns `void` (Kotlin `Unit`). */
    val returnsVoid: Boolean = executable is Method && executable.returnType == Void.TYPE

    fun canHandle(payload: Class<*>): Boolean = payloadType.isAssignableFrom(payload)

    /** Invokes the handler on its Spring bean. */
    fun invoke(message: Message<*>): Any? = invokeOn(checkNotNull(bean) { "$this is not bound to a bean" }, message)

    /** Invokes the handler method on [instance]; unwraps reflection wrappers so callers see the original exception. */
    fun invokeOn(instance: Any, message: Message<*>): Any? = reflective { (target as Method).invoke(instance, *args(message)) }

    /** Invokes the handler constructor, returning the new instance. */
    fun construct(message: Message<*>): Any = reflective { (target as Constructor<*>).newInstance(*args(message)) }

    private fun args(message: Message<*>) = Array(resolvers.size) { resolvers[it].resolve(message) }

    private inline fun <T> reflective(block: () -> T): T =
        try {
            block()
        } catch (e: InvocationTargetException) {
            throw asRuntime(e.targetException)
        }

    override fun toString(): String =
        "${executable.declaringClass.simpleName}.${if (executable is Constructor<*>) "<init>" else executable.name}" +
            "(${payloadType.simpleName})"

    companion object {

        fun create(
            executable: Executable,
            declaredPayloadType: Class<*>,
            beanFactory: BeanFactory,
            bean: Any? = null,
        ): HandlerMethod {
            require(executable.parameterCount > 0) { "Handler $executable must declare at least one parameter (the payload)" }
            val first = executable.parameterTypes[0]
            val payloadFromParam = !Message::class.java.isAssignableFrom(first)
            val payloadType = when {
                declaredPayloadType != Any::class.java -> declaredPayloadType
                payloadFromParam -> first
                else -> Any::class.java
            }
            val resolvers = (0 until executable.parameterCount).map { index ->
                val parameter = MethodParameter.forExecutable(executable, index)
                if (index == 0 && payloadFromParam) ParameterResolver { it.payload }
                else resolverFor(parameter, beanFactory)
            }
            return HandlerMethod(executable, payloadType, resolvers, bean)
        }

        private fun resolverFor(parameter: MethodParameter, beanFactory: BeanFactory): ParameterResolver {
            val type = parameter.parameterType
            AnnotatedElementUtils.findMergedAnnotation(parameter.parameter, MetaDataValue::class.java)?.let { ann ->
                return ParameterResolver { message ->
                    message.metaData[ann.value].also {
                        if (it == null && ann.required) {
                            throw IllegalArgumentException("Required metadata '${ann.value}' missing on ${message.payloadType.name}")
                        }
                    }
                }
            }
            when {
                parameter.hasParameterAnnotation(MessageIdentifier::class.java) ->
                    return ParameterResolver { it.identifier }
                parameter.hasParameterAnnotation(Timestamp::class.java) ->
                    return ParameterResolver { (it as? EventMessage<*>)?.timestamp ?: Instant.now() }
                parameter.hasParameterAnnotation(SequenceNumber::class.java) ->
                    return ParameterResolver { (it as? DomainEventMessage<*>)?.sequenceNumber }
                parameter.hasParameterAnnotation(SourceId::class.java) ->
                    return ParameterResolver { (it as? DomainEventMessage<*>)?.aggregateIdentifier }
                parameter.hasParameterAnnotation(AggregateType::class.java) ->
                    return ParameterResolver { (it as? DomainEventMessage<*>)?.type }
                Message::class.java.isAssignableFrom(type) ->
                    return ParameterResolver { it }
                type == MetaData::class.java || type == Map::class.java ->
                    return ParameterResolver { it.metaData }
            }
            // Spring bean injection, resolved lazily so handler beans may depend on beans created later.
            val provider = beanFactory.getBeanProvider(type)
            return ParameterResolver { provider.getObject() }
        }

        /** Mirrors DefaultCommandGateway#asRuntime: runtime exceptions and errors pass through untouched. */
        fun asRuntime(e: Throwable): RuntimeException = when (e) {
            is Error -> throw e
            is RuntimeException -> e
            else -> CommandExecutionException("An exception occurred while executing a command", e)
        }

        /** Most specific handler (deepest payload type) among [handlers] able to handle [payloadType]. */
        fun mostSpecific(handlers: List<HandlerMethod>, payloadType: Class<*>): HandlerMethod? =
            handlers.filter { it.canHandle(payloadType) }
                .reduceOrNull { best, candidate ->
                    if (best.payloadType.isAssignableFrom(candidate.payloadType)) candidate else best
                }
    }
}
