package app.dc8.axonthin.api

/**
 * What the current chunk has done that projections do not show yet — for validation that runs while commands are
 * handled (command handlers, services they call). Inject it as a Spring bean or declare it as a handler parameter.
 *
 * In a chunk, projections receive the events only after the last command. Validation that queries a projection
 * must therefore also look at [pendingEvents]: "the database, plus what this chunk changed so far".
 *
 * Engines:
 * - axon-thin: pending events are those applied/published by earlier commands of the chunk (and by the current one
 *   so far); [aggregate] returns the chunk's in-memory aggregates.
 * - Axon 4 (v4 adapter): projections are updated after every command, so [pendingEvents] is always empty and the
 *   same validation code stays correct; [aggregate] returns `null`.
 *
 * Outside a chunk (no command being handled on this thread) everything is empty and [active] is `false`.
 */
interface ChunkContext {

    /** A command is being handled on this thread (a chunk of one for `sendAndWait`). */
    val active: Boolean

    /** Payloads of events not yet seen by projections, in the order they were applied/published. */
    fun pendingEvents(): List<Any>

    /** [pendingEvents] of the given type (including subtypes), in order. */
    fun <T : Any> pendingEvents(type: Class<T>): List<T> = pendingEvents().filterIsInstance(type)

    /** Payloads of all top-level commands of the chunk, in order (nested commands are not listed). */
    fun commands(): List<Any>

    /** Index in [commands] of the top-level command being handled, or -1. */
    fun currentCommandIndex(): Int

    /**
     * The chunk's in-memory aggregate root of [type] with [id], if the chunk loaded or created it; never queries the
     * database. Read-only: change aggregates through commands only.
     */
    fun <T : Any> aggregate(type: Class<T>, id: String): T?
}

/** Kotlin: `chunk.pendingEvents<TaskCreatedEvent>()`. */
inline fun <reified T : Any> ChunkContext.pendingEvents(): List<T> = pendingEvents(T::class.java)

/** Kotlin: `chunk.aggregate<Container>(id)`. */
inline fun <reified T : Any> ChunkContext.aggregate(id: String): T? = aggregate(T::class.java, id)
