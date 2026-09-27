package app.dc8.axonthin

import app.dc8.axonthin.aggregate.AggregateCommandHandler
import app.dc8.axonthin.aggregate.AggregateModel
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
                eventHandlerBeans += EventHandlingBean(name, bean, handlers)
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

    internal fun aggregateModel(type: Class<*>): AggregateModel? = aggregates.firstOrNull { it.rootType == type }

    /** Beans with event handlers, in `@Order` order. */
    internal val eventHandlingBeans: List<EventHandlingBean> get() = eventHandlerBeans

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

    /** A bean's event handlers; per payload type the most specific one (single or batch) is used. */
    internal class EventHandlingBean(val beanName: String, val bean: Any, private val handlers: List<HandlerMethod>) {
        private val routes = ConcurrentHashMap<Class<*>, Any>()

        fun handlerFor(payloadType: Class<*>): HandlerMethod? =
            routes.computeIfAbsent(payloadType) { HandlerMethod.mostSpecific(handlers, it) ?: NONE } as? HandlerMethod

        /** `@ProcessingGroup` name if present (Axon's projection identity), else the bean name. */
        val projectionName: String = ClassUtils.getUserClass(bean).annotations
            .firstOrNull { it.annotationClass.qualifiedName == "org.axonframework.config.ProcessingGroup" }
            ?.let { runCatching { it.annotationClass.java.getMethod("value").invoke(it) as String }.getOrNull() }
            ?: beanName

        /** Whether any handler of this bean handles [payloadType]. */
        fun handles(payloadType: Class<*>): Boolean = handlerFor(payloadType) != null

        override fun toString(): String = bean.javaClass.simpleName
    }

    private companion object {
        val NONE = Any()
    }
}
