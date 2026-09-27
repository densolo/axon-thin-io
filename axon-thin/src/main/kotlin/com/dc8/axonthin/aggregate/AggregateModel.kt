package com.dc8.axonthin.aggregate

import com.dc8.axonthin.HandlerMethod
import org.axonframework.commandhandling.CommandHandler
import org.axonframework.eventhandling.EventHandler
import org.axonframework.modelling.command.AggregateCreationPolicy
import org.axonframework.modelling.command.AggregateIdentifier
import org.axonframework.modelling.command.AggregateRoot
import org.axonframework.modelling.command.CreationPolicy
import org.springframework.beans.factory.BeanFactory
import org.springframework.core.annotation.AnnotatedElementUtils
import org.springframework.util.ReflectionUtils
import java.lang.reflect.AccessibleObject
import java.lang.reflect.Constructor
import java.lang.reflect.Field
import java.lang.reflect.Member
import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap

/**
 * Introspected `@Aggregate` / `@AggregateRoot` class (event-sourced).
 *
 * Supported: `@AggregateIdentifier` (field or getter), `@CommandHandler` on constructors
 * and methods, `@CreationPolicy`, `@EventSourcingHandler` (any `@EventHandler` inside the aggregate),
 * `AggregateLifecycle.apply / isLive / getVersion / markDeleted`.
 * Like Axon's event-sourced aggregates, a `@AggregateVersion` field is never written (use `getVersion()`).
 * Not (yet): `@AggregateMember` entities, `AggregateLifecycle.createNew`, snapshots, polymorphic aggregates.
 */
internal class AggregateModel(
    val rootType: Class<*>,
    private val beanName: String?,
    private val beanFactory: BeanFactory,
) {
    /** Aggregate type as stored in the `type` column: `@Aggregate(type = ...)` or the simple class name. */
    val typeName: String = declaredType(rootType) ?: rootType.simpleName

    private val identifierMember: Member = findMember(AggregateIdentifier::class.java)
        ?: throw IllegalStateException("Aggregate ${rootType.name} has no @AggregateIdentifier field or method")

    private val noArgConstructor: Constructor<*>? =
        runCatching { rootType.getDeclaredConstructor().also { it.trySetAccessible() } }.getOrNull()

    val sourcingHandlers: List<HandlerMethod> = methods(EventHandler::class.java).map { method ->
        val ann = AnnotatedElementUtils.findMergedAnnotation(method, EventHandler::class.java)!!
        HandlerMethod.create(method, ann.payloadType.java, beanFactory)
    }

    val commandHandlers: List<AggregateCommandHandler> = run {
        val constructors = rootType.declaredConstructors
            .filter { !it.isSynthetic && AnnotatedElementUtils.hasAnnotation(it, CommandHandler::class.java) }
            .map { ctor -> commandHandler(ctor, AggregateCreationPolicy.ALWAYS, isConstructor = true) }
        val methods = methods(CommandHandler::class.java).map { method ->
            val policy = AnnotatedElementUtils.findMergedAnnotation(method, CreationPolicy::class.java)?.value
                ?: AggregateCreationPolicy.NEVER
            commandHandler(method, policy, isConstructor = false)
        }
        constructors + methods
    }

    private val sourcingRoutes = ConcurrentHashMap<Class<*>, Any>()

    fun sourcingHandler(payloadType: Class<*>): HandlerMethod? =
        sourcingRoutes.computeIfAbsent(payloadType) { HandlerMethod.mostSpecific(sourcingHandlers, it) ?: NONE } as? HandlerMethod

    /** New empty instance for event sourcing / creation policies: Spring prototype bean if registered (injection), else no-arg constructor. */
    fun newInstance(): Any =
        if (beanName != null && beanFactory.isPrototype(beanName)) beanFactory.getBean(beanName)
        else checkNotNull(noArgConstructor) { "Aggregate ${rootType.name} needs a no-arg constructor" }.newInstance()

    fun identifierOf(root: Any): Any? = when (val m = identifierMember) {
        is Field -> m.get(root)
        is Method -> m.invoke(root)
        else -> null
    }

    private fun commandHandler(
        executable: java.lang.reflect.Executable,
        policy: AggregateCreationPolicy,
        isConstructor: Boolean,
    ): AggregateCommandHandler {
        val ann = AnnotatedElementUtils.findMergedAnnotation(executable, CommandHandler::class.java)!!
        val handler = HandlerMethod.create(executable, ann.payloadType.java, beanFactory)
        val commandName = ann.commandName.ifEmpty { handler.payloadType.name }
        return AggregateCommandHandler(this, commandName, handler, policy, isConstructor)
    }

    private fun methods(annotation: Class<out Annotation>): List<Method> =
        ReflectionUtils.getUniqueDeclaredMethods(rootType) { !it.isBridge && !it.isSynthetic }
            .filter { AnnotatedElementUtils.hasAnnotation(it, annotation) }
            .onEach { it.trySetAccessible() }

    private fun findMember(annotation: Class<out Annotation>): Member? {
        var type: Class<*>? = rootType
        while (type != null && type != Any::class.java) {
            type.declaredFields.firstOrNull { AnnotatedElementUtils.hasAnnotation(it, annotation) }
                ?.let { return it.accessible() }
            type.declaredMethods.firstOrNull { it.parameterCount == 0 && AnnotatedElementUtils.hasAnnotation(it, annotation) }
                ?.let { return it.accessible() }
            type = type.superclass
        }
        return null
    }

    private fun <T : AccessibleObject> T.accessible(): T = also { it.trySetAccessible() }

    override fun toString(): String = "Aggregate[$typeName]"

    companion object {
        private val NONE = Any()

        fun isAggregate(type: Class<*>): Boolean = AnnotatedElementUtils.hasAnnotation(type, AggregateRoot::class.java)

        /** `type` attribute of @Aggregate / @AggregateRoot (no @AliasFor in Axon, so read it directly). */
        private fun declaredType(type: Class<*>): String? =
            type.annotations.asSequence()
                .filter { it is AggregateRoot || it.annotationClass.java.isAnnotationPresent(AggregateRoot::class.java) }
                .mapNotNull { ann -> runCatching { ann.annotationClass.java.getMethod("type").invoke(ann) as String }.getOrNull() }
                .firstOrNull { it.isNotEmpty() }
    }
}
