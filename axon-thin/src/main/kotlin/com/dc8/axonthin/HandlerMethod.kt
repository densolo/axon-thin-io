package com.dc8.axonthin

import org.axonframework.commandhandling.CommandExecutionException
import org.axonframework.eventhandling.EventMessage
import org.axonframework.eventhandling.Timestamp
import org.axonframework.messaging.Message
import org.axonframework.messaging.MetaData
import org.axonframework.messaging.annotation.MessageIdentifier
import org.axonframework.messaging.annotation.MetaDataValue
import org.springframework.aop.support.AopUtils
import org.springframework.beans.factory.BeanFactory
import org.springframework.core.MethodParameter
import org.springframework.core.annotation.AnnotatedElementUtils
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.time.Instant

/** Resolves one handler argument from the message being handled. */
internal fun interface ParameterResolver {
    fun resolve(message: Message<*>): Any?
}

/**
 * A single `@CommandHandler` / `@EventHandler` method bound to its Spring bean.
 *
 * Supported parameters (a subset of Axon 4's default resolvers):
 * - first parameter: the payload (or the full [Message] if it is declared as a Message type)
 * - any [Message] subtype (CommandMessage, EventMessage, ...)
 * - [MetaData], `@MetaDataValue`, `@MessageIdentifier`, `@Timestamp` (events only, `Instant`)
 * - anything else: a Spring bean of that type (Axon's SpringBeanParameterResolverFactory)
 */
internal class HandlerMethod(
    val bean: Any,
    val method: Method,
    val payloadType: Class<*>,
    private val resolvers: List<ParameterResolver>,
) {
    private val invocable: Method = AopUtils.selectInvocableMethod(method, bean.javaClass).also { it.trySetAccessible() }

    fun canHandle(payload: Class<*>): Boolean = payloadType.isAssignableFrom(payload)

    /** Invokes the handler; unwraps reflection wrappers so callers see the exception the handler threw. */
    fun invoke(message: Message<*>): Any? {
        val args = Array(resolvers.size) { resolvers[it].resolve(message) }
        try {
            return invocable.invoke(bean, *args)
        } catch (e: InvocationTargetException) {
            throw asRuntime(e.targetException)
        }
    }

    override fun toString(): String = "${method.declaringClass.simpleName}.${method.name}(${payloadType.simpleName})"

    companion object {

        fun create(
            bean: Any,
            method: Method,
            declaredPayloadType: Class<*>,
            beanFactory: BeanFactory,
        ): HandlerMethod {
            require(method.parameterCount > 0) { "Handler $method must declare at least one parameter (the payload)" }
            val first = method.parameterTypes[0]
            val payloadFromParam = !Message::class.java.isAssignableFrom(first)
            val payloadType = when {
                declaredPayloadType != Any::class.java -> declaredPayloadType
                payloadFromParam -> first
                else -> Any::class.java
            }
            val resolvers = (0 until method.parameterCount).map { index ->
                val parameter = MethodParameter(method, index)
                if (index == 0 && payloadFromParam) ParameterResolver { it.payload }
                else resolverFor(parameter, beanFactory)
            }
            return HandlerMethod(bean, method, payloadType, resolvers)
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
            if (parameter.hasParameterAnnotation(MessageIdentifier::class.java)) {
                return ParameterResolver { it.identifier }
            }
            if (parameter.hasParameterAnnotation(Timestamp::class.java)) {
                return ParameterResolver { (it as? EventMessage<*>)?.timestamp ?: Instant.now() }
            }
            if (Message::class.java.isAssignableFrom(type)) {
                return ParameterResolver { it }
            }
            if (type == MetaData::class.java || type == Map::class.java) {
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
    }
}
