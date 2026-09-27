package app.dc8.example.task

import org.axonframework.eventsourcing.EventCountSnapshotTriggerDefinition
import org.axonframework.eventsourcing.SnapshotTriggerDefinition
import org.axonframework.eventsourcing.Snapshotter
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 * Standard Axon snapshot configuration, engine-agnostic: the Snapshotter bean is Axon's SpringAggregateSnapshotter
 * on Axon 4 and ThinSnapshotter on axon-thin.
 */
@Configuration(proxyBeanMethods = false)
class TaskModelConfiguration {

    @Bean
    fun taskSnapshotTrigger(
        snapshotter: Snapshotter,
        @Value("\${task.snapshot-threshold:100}") threshold: Int,
    ): SnapshotTriggerDefinition = EventCountSnapshotTriggerDefinition(snapshotter, threshold)
}
