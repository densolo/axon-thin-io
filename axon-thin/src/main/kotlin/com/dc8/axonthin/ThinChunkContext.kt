package com.dc8.axonthin

import com.dc8.axonthin.api.ChunkContext

/** [ChunkContext] backed by the thread's unit of work (the running chunk). */
class ThinChunkContext : ChunkContext {

    override val active: Boolean get() = ThinUnitOfWork.currentOrNull() != null

    override fun pendingEvents(): List<Any> =
        ThinUnitOfWork.currentOrNull()?.pendingEventsSnapshot?.map { it.payload } ?: emptyList()

    override fun commands(): List<Any> =
        ThinUnitOfWork.currentOrNull()?.chunkCommands?.map { it.payload } ?: emptyList()

    override fun currentCommandIndex(): Int = ThinUnitOfWork.currentOrNull()?.currentCommandIndex ?: -1

    override fun <T : Any> aggregate(type: Class<T>, id: String): T? {
        val uow = ThinUnitOfWork.currentOrNull() ?: return null
        return uow.aggregates.entries.firstOrNull { (key, aggregate) ->
            key.second == id && !aggregate.deleted && type.isInstance(aggregate.root)
        }?.value?.root?.let(type::cast)
    }
}
