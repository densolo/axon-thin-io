package app.dc8.axonthin.api

/**
 * Dispatches a chunk of commands in one transaction.
 *
 * Semantics (thin runtime):
 * - commands are handled in list order, on the calling thread, against in-memory aggregates: a later command sees
 *   the aggregate state produced by earlier commands of the same chunk;
 * - aggregates targeted by the chunk are loaded up front with one query for snapshots and one for events;
 * - all events of the chunk are appended with one batch insert, then handed to event handlers — batch handlers
 *   (`@EventHandler fun on(events: List<…>)`) receive them as one ordered list;
 * - projections therefore reflect the chunk only after its last command: validation inside a chunk must look at
 *   the chunk's own changes in addition to the projections;
 * - all-or-nothing: the first failure rolls the whole chunk back and is rethrown as-is.
 *
 * Elements may be plain payloads or pre-built Axon `CommandMessage`s (e.g. to carry metadata).
 *
 * [R] works like Axon's `<R> R sendAndWait(command)`: an unchecked cast of the handler results, taken from the expected
 * type or given explicitly — `val ids: List<String> = sendAllAndWait(creates)` or `sendAllAndWait<String>(creates)`.
 * A mismatch surfaces as ClassCastException where an element is used; for chunks mixing commands with different
 * results use `sendAllAndWait<Any?>(…)`.
 *
 * @return the handler results, in the same order as [commands] (`null` for `Unit`/`void` handlers)
 */
interface BulkCommandGateway {

    fun <R> sendAllAndWait(commands: List<Any>): List<R> = sendAllAndWait(commands, BulkOptions.DEFAULT)

    fun <R> sendAllAndWait(commands: List<Any>, options: BulkOptions): List<R>
}

data class BulkOptions(
    /**
     * How often to re-run the whole chunk when another writer appended to one of its aggregates first
     * (Axon `ConcurrencyException`). A re-run reloads the aggregates, so deltas are applied on top of the latest
     * state. Only applies when the chunk owns its transaction; inside a caller's transaction it is rethrown.
     */
    val concurrencyRetries: Int = 0,
) {
    init {
        require(concurrencyRetries >= 0) { "concurrencyRetries must be >= 0" }
    }

    companion object {
        @JvmField
        val DEFAULT = BulkOptions()
    }
}
