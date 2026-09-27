package com.dc8.axonthin

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

/**
 * Discovers `@CommandHandler` / `@EventHandler` methods on singleton Spring beans once the context is ready,
 * the same way Axon's Spring integration registers annotated beans as handlers.
 *
 * Out of scope (for now): aggregates, sagas, query handlers, handler interceptors, processing-group routing.
 */
class ThinHandlerRegistry : SmartInitializingSingleton, BeanFactoryAware {

    private val log = LoggerFactory.getLogger(ThinHandlerRegistry::class.java)

    private lateinit var beanFactory: ConfigurableListableBeanFactory
    private val commandHandlers = HashMap<String, HandlerMethod>()
    private val eventHandlerBeans = ArrayList<EventHandlingBean>()
    private val eventRoutes = ConcurrentHashMap<Class<*>, List<HandlerMethod>>()

    override fun setBeanFactory(beanFactory: BeanFactory) {
        this.beanFactory = beanFactory as ConfigurableListableBeanFactory
    }

    override fun afterSingletonsInstantiated() {
        for (name in beanFactory.beanDefinitionNames) {
            val definition = beanFactory.getMergedBeanDefinition(name)
            if (!definition.isSingleton || definition.isAbstract) continue
            val type = beanFactory.getType(name, false) ?: continue
            val userType = ClassUtils.getUserClass(type)
            val commandMethods = annotatedMethods(userType, CommandHandler::class.java)
            val eventMethods = annotatedMethods(userType, EventHandler::class.java)
            if (commandMethods.isEmpty() && eventMethods.isEmpty()) continue

            val bean = beanFactory.getBean(name)
            commandMethods.forEach { registerCommandHandler(bean, it) }
            if (eventMethods.isNotEmpty()) {
                val handlers = eventMethods.map { method ->
                    val ann = AnnotatedElementUtils.findMergedAnnotation(method, EventHandler::class.java)!!
                    HandlerMethod.create(bean, method, ann.payloadType.java, beanFactory)
                }
                eventHandlerBeans += EventHandlingBean(bean, handlers)
            }
        }
        eventHandlerBeans.sortWith { a, b -> AnnotationAwareOrderComparator.INSTANCE.compare(a.bean, b.bean) }
        log.info(
            "axon-thin registered {} command handler(s) and {} event handling bean(s)",
            commandHandlers.size, eventHandlerBeans.size,
        )
    }

    internal fun commandHandler(commandName: String): HandlerMethod? = commandHandlers[commandName]

    /** All event handlers for [payloadType]: at most one (the most specific) per bean, in bean order. */
    internal fun eventHandlers(payloadType: Class<*>): List<HandlerMethod> =
        eventRoutes.computeIfAbsent(payloadType) { type -> eventHandlerBeans.mapNotNull { it.mostSpecific(type) } }

    private fun registerCommandHandler(bean: Any, method: Method) {
        val ann = AnnotatedElementUtils.findMergedAnnotation(method, CommandHandler::class.java)!!
        val handler = HandlerMethod.create(bean, method, ann.payloadType.java, beanFactory)
        val commandName = ann.commandName.ifEmpty { handler.payloadType.name }
        val existing = commandHandlers.putIfAbsent(commandName, handler)
        check(existing == null) { "Duplicate command handler for '$commandName': $existing and $handler" }
    }

    private fun annotatedMethods(type: Class<*>, annotation: Class<out Annotation>): List<Method> =
        ReflectionUtils.getUniqueDeclaredMethods(type) { !it.isBridge && !it.isSynthetic }
            .filter { AnnotatedElementUtils.hasAnnotation(it, annotation) }

    private class EventHandlingBean(val bean: Any, val handlers: List<HandlerMethod>) {
        fun mostSpecific(payloadType: Class<*>): HandlerMethod? =
            handlers.filter { it.canHandle(payloadType) }
                .reduceOrNull { best, candidate ->
                    if (best.payloadType.isAssignableFrom(candidate.payloadType)) candidate else best
                }
    }
}
