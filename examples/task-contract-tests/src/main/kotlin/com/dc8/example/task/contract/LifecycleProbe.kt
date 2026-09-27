package com.dc8.example.task.contract

import org.axonframework.commandhandling.CommandHandler
import org.axonframework.eventsourcing.EventSourcingHandler
import org.axonframework.modelling.command.AggregateIdentifier
import org.axonframework.modelling.command.AggregateLifecycle.apply
import org.axonframework.modelling.command.AggregateLifecycle.getVersion
import org.axonframework.modelling.command.AggregateLifecycle.isLive
import org.axonframework.modelling.command.AggregateVersion
import org.axonframework.modelling.command.TargetAggregateIdentifier
import org.axonframework.spring.stereotype.Aggregate

data class StartProbeCommand(val probeId: String)
data class PokeProbeCommand(@TargetAggregateIdentifier val probeId: String)

data class ProbeStartedEvent(val probeId: String)
data class ProbeStepEvent(val probeId: String, val step: String, val version: Long?)
data class ProbePokedEvent(val probeId: String, val live: Boolean)

/**
 * Test-only aggregate exercising AggregateLifecycle corner cases: apply from a constructor, apply from inside an
 * event sourcing handler, andThenApply, isLive() during replay, getVersion() and @AggregateVersion.
 * The expected event order is what Axon 4 produces.
 */
@Aggregate
class LifecycleProbe() {

    @AggregateIdentifier
    private var probeId: String? = null

    @AggregateVersion
    private var version: Long? = null

    private var sourcedWhileNotLive = 0

    @CommandHandler
    constructor(command: StartProbeCommand) : this() {
        apply(ProbeStartedEvent(command.probeId))
            .andThenApply { ProbeStepEvent(command.probeId, "and-then-apply", getVersion()) }
    }

    /** Returns what the aggregate observed: replayed events seen while not live, and its version before/after. */
    @CommandHandler
    fun handle(command: PokeProbeCommand): String {
        val before = version
        apply(ProbePokedEvent(command.probeId, isLive()))
        return "replayed=$sourcedWhileNotLive before=$before after=$version"
    }

    @EventSourcingHandler
    fun on(event: ProbeStartedEvent) {
        probeId = event.probeId
        if (!isLive()) sourcedWhileNotLive++
        // nested apply: must be delayed until this handler returns
        apply(ProbeStepEvent(event.probeId, "from-sourcing-handler", getVersion()))
    }

    @EventSourcingHandler
    fun on(@Suppress("UNUSED_PARAMETER") event: ProbeStepEvent) {
        if (!isLive()) sourcedWhileNotLive++
    }
}
