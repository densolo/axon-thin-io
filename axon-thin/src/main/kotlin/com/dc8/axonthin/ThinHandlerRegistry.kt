package com.dc8.axonthin

import com.dc8.axonthin.aggregate.AggregateCommandHandler
import com.dc8.axonthin.aggregate.AggregateModel
import org.axonframework.commandhandling.CommandHandler
import org.axonframework.eventhandling.EventHandler
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.BeanFactoryAware
import org.springframework.beans.factory.SmartInitializingSingleton
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.core.annotation.AnnotatedElementUtils
import org.springframework.core.annotation.AnnotationAwareOrderComparator
import org.springframework.util.ClassUtils
import org.springframework.util.ReflectionUtils
import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap

/** Where a command goes: a method on a Spring singleton, or an aggregate. */
internal sealed interface CommandRoute {
    class Bean(val handler: HandlerMethod) : CommandRoute {
        override fun toString() = handler.toString()
    }

    class Aggregate(val handler: AggregateCommandHandler) : CommandRoute {
        override fun toString() = handler.toString()
    }
}

/**
 * Discovers handlers once the context is ready, like Axon's Spring integration:
 * - `@CommandHandler` / `@EventHandler` methods on singleton beans;
 * - `@Aggregate` (`@AggregateRoot`) bean definitions — prototype-scoped — as event-sourced aggregates.
 *
 * Every event handler bean is invoked for every event (all processing groups behave as one subscribing group).
 * Out of scope: sagas, query handlers, handler interceptors, aggregate members.
 */
class ThinHandlerRegistry : SmartInitializingSingleton, BeanFactoryAware {

    private val log = LoggerFactory.getLogger(ThinHandlerRegistry::class.java)

    private lateinit var beanFactory: ConfigurableListableBeanFactory
    private val commandRoutes = HashMap<String, CommandRoute>()
    private val eventHandlerBeans = ArrayList<EventHandlingBean>()
    private val eventRoutes = ConcurrentHashMap<Class<*>, List<HandlerMethod>>()
    private val aggregates = ArrayList<AggregateModel>()

    override fun setBeanFactory(beanFactory: BeanFactory) {
        this.beanFactory = beanFactory as ConfigurableListableBeanFactory
    }

    override fun afterSingletonsInstantiated() {
        for (name in beanFactory.beanDefinitionNames) {
            val definition = beanFactory.getMergedBeanDefinition(name)
            if (definition.isAbstract) continue
            val type = beanFactory.getType(name, false) ?: continue
            val userType = ClassUtils.getUserClass(type)
            if (AggregateModel.isAggregate(userType)) {
                registerAggregate(AggregateModel(userType, name, beanFactory))
                continue
            }
            if (!definition.isSingleton) continue
            val commandMethods = annotatedMethods(userType, CommandHandler::class.java)
            val eventMethods = annotatedMethods(userType, EventHandler::class.java)
            if (commandMethods.isEmpty() && eventMethods.isEmpty()) continue

            val bean = beanFactory.getBean(name)
            commandMethods.forEach { registerBeanCommandHandler(bean, it) }
            if (eventMethods.isNotEmpty()) {
                val handlers = eventMethods.map { method ->
                    val ann = AnnotatedElementUtils.findMergedAnnotation(method, EventHandler::class.java)!!
                    HandlerMethod.create(method, ann.payloadType.java, beanFactory, bean)
                }
                eventHandlerBeans += EventHandlingBean(bean, handlers)
            }
        }
        eventHandlerBeans.sortWith { a, b -> AnnotationAwareOrderComparator.INSTANCE.compare(a.bean, b.bean) }
        log.info(
            "axon-thin registered {} command handler(s), {} aggregate(s) {} and {} event handling bean(s)",
            commandRoutes.size, aggregates.size, aggregates.map { it.typeName }, eventHandlerBeans.size,
        )
    }

    internal fun commandRoute(commandName: String): CommandRoute? = commandRoutes[commandName]

    internal val hasAggregates: Boolean get() = aggregates.isNotEmpty()

    /** All event handlers for [payloadType]: at most one (the most specific) per bean, in bean order. */
    internal fun eventHandlers(payloadType: Class<*>): List<HandlerMethod> =
        eventRoutes.computeIfAbsent(payloadType) { type -> eventHandlerBeans.mapNotNull { it.mostSpecific(type) } }

    private fun registerAggregate(model: AggregateModel) {
        aggregates += model
        model.commandHandlers.forEach { register(it.commandName, CommandRoute.Aggregate(it)) }
    }

    private fun registerBeanCommandHandler(bean: Any, method: Method) {
        val ann = AnnotatedElementUtils.findMergedAnnotation(method, CommandHandler::class.java)!!
        val handler = HandlerMethod.create(method, ann.payloadType.java, beanFactory, bean)
        register(ann.commandName.ifEmpty { handler.payloadType.name }, CommandRoute.Bean(handler))
    }

    private fun register(commandName: String, route: CommandRoute) {
        val existing = commandRoutes.putIfAbsent(commandName, route)
        check(existing == null) { "Duplicate command handler for '$commandName': $existing and $route" }
    }

    private fun annotatedMethods(type: Class<*>, annotation: Class<out Annotation>): List<Method> =
        ReflectionUtils.getUniqueDeclaredMethods(type) { !it.isBridge && !it.isSynthetic }
            .filter { AnnotatedElementUtils.hasAnnotation(it, annotation) }

    private class EventHandlingBean(val bean: Any, val handlers: List<HandlerMethod>) {
        fun mostSpecific(payloadType: Class<*>): HandlerMethod? = HandlerMethod.mostSpecific(handlers, payloadType)
    }
}
