package com.dc8.axonthin.api

/**
 * Dispatches a batch of commands inside a single transaction.
 *
 * Semantics (identical for the thin runtime and the Axon 4 adapter):
 * - commands are handled sequentially, in list order, on the calling thread;
 * - each command behaves like `CommandGateway.sendAndWait`: its events are dispatched to the (subscribing)
 *   event handlers before the next command runs, so later commands and projections see earlier effects;
 * - everything joins one transaction (or the caller's transaction, if one is active);
 * - the first failure aborts the batch, rolls the whole transaction back and is rethrown as-is.
 *
 * Elements may be plain payloads or pre-built Axon `CommandMessage`s (e.g. to carry metadata).
 *
 * @return the handler results, in the same order as [commands] (`null` for `Unit`/`void` handlers)
 */
interface BulkCommandGateway {

    fun sendAllAndWait(commands: List<Any>): List<Any?>
}
