package app.dc8.axonthin.eventstore

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.JdbcUtils
import java.sql.DatabaseMetaData
import javax.sql.DataSource

/** Supplies `global_index` values; `null` means the column generates them (identity). */
internal interface GlobalIndexAllocator {

    /** [count] values, in increasing order; `null` when the database generates them. */
    fun allocate(count: Int): List<Long>?

    companion object {
        val IDENTITY = object : GlobalIndexAllocator {
            override fun allocate(count: Int): List<Long>? = null
        }
    }
}

/**
 * Hibernate "pooled" optimizer semantics: every `nextval` = V reserves the block (V - allocationSize, V].
 * Blocks handed out to Hibernate (Axon 4) and to thin never overlap, so both can write the same table.
 *
 * All blocks a chunk needs are fetched in one round trip.
 */
internal class PooledSequenceAllocator(
    private val jdbc: JdbcTemplate,
    dataSource: DataSource,
    sequenceName: String,
    private val allocationSize: Int,
) : GlobalIndexAllocator {

    private val nextValuesSql: String = run {
        val product = JdbcUtils.extractDatabaseMetaData(dataSource, DatabaseMetaData::getDatabaseProductName)
        if (product.contains("PostgreSQL", ignoreCase = true)) "select nextval('$sequenceName') from generate_series(1, ?)"
        else "select next value for $sequenceName from system_range(1, ?)" // H2
    }

    private var next = 0L
    private var high = -1L

    init {
        require(allocationSize > 0) { "allocationSize must be positive" }
    }

    @Synchronized
    override fun allocate(count: Int): List<Long> {
        val result = ArrayList<Long>(count)
        while (result.size < count && next <= high) result += next++
        // usually one round trip; a second only when the sequence's very first block is short (values below 1)
        while (result.size < count) {
            val blocks = (count - result.size + allocationSize - 1) / allocationSize
            val highs = jdbc.queryForList(nextValuesSql, Long::class.java, blocks)
            for (h in highs) {
                high = h
                next = maxOf(1, h - allocationSize + 1)
                while (result.size < count && next <= high) result += next++
            }
        }
        return result
    }
}
