package com.dc8.axonthin.eventstore

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.JdbcUtils
import java.sql.DatabaseMetaData
import javax.sql.DataSource

/** Supplies `global_index` values; `null` means the column generates them (identity). */
internal fun interface GlobalIndexAllocator {
    fun next(): Long?

    companion object {
        val IDENTITY = GlobalIndexAllocator { null }
    }
}

/**
 * Hibernate "pooled" optimizer semantics: every `nextval` = V reserves the block (V - allocationSize, V].
 * Blocks handed out to Hibernate (Axon 4) and to thin never overlap, so both can write the same table.
 */
internal class PooledSequenceAllocator(
    private val jdbc: JdbcTemplate,
    dataSource: DataSource,
    sequenceName: String,
    private val allocationSize: Int,
) : GlobalIndexAllocator {

    private val nextValueSql: String = run {
        val product = JdbcUtils.extractDatabaseMetaData(dataSource, DatabaseMetaData::getDatabaseProductName)
        if (product.contains("PostgreSQL", ignoreCase = true)) "select nextval('$sequenceName')"
        else "select next value for $sequenceName" // H2, HSQLDB, SQL Server, ...
    }

    private var next = 0L
    private var high = -1L

    init {
        require(allocationSize > 0) { "allocationSize must be positive" }
    }

    @Synchronized
    override fun next(): Long {
        if (next > high) {
            high = jdbc.queryForObject(nextValueSql, Long::class.java)!!
            next = maxOf(1, high - allocationSize + 1)
        }
        return next++
    }
}
