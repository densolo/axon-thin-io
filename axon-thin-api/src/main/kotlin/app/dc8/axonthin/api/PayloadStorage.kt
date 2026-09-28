package app.dc8.axonthin.api

import java.sql.ResultSet
import javax.sql.DataSource

/**
 * How the `payload` / `meta_data` columns of Axon's event tables store the serialized bytes.
 *
 * | Storage | Column types | Read | Write |
 * |---|---|---|---|
 * | [BINARY] | `bytea`, `blob`, `varbinary` | bytes | bytes |
 * | [OID] | PostgreSQL `oid` (large objects, Hibernate's default for `@Lob byte[]`) | `lo_get(column)` | `lo_from_bytea(0, ?)` |
 * | [TEXT] | `text`, `varchar`, `clob` | UTF-8 string | UTF-8 string |
 *
 * Detected per column at startup ([detect]); a restart after converting the columns picks up the new type.
 */
enum class PayloadStorage {
    BINARY,
    OID,
    TEXT,
    ;

    /** Select-list expression for [column] (optionally qualified with [alias]), labelled [column]. */
    fun select(column: String, alias: String? = null): String {
        val qualified = if (alias == null) column else "$alias.$column"
        return if (this == OID) "lo_get($qualified) as $column" else qualified
    }

    /** Insert placeholder for a value of this column. */
    val placeholder: String get() = if (this == OID) "lo_from_bytea(0, ?)" else "?"

    /** JDBC parameter for the serialized [bytes]. */
    fun bind(bytes: ByteArray?): Any? = if (this == TEXT) bytes?.toString(Charsets.UTF_8) else bytes

    /** The serialized bytes of [column] in the current row. */
    fun read(rs: ResultSet, column: String): ByteArray? =
        if (this == TEXT) rs.getString(column)?.toByteArray(Charsets.UTF_8) else rs.getBytes(column)

    companion object {
        /**
         * The storage of [column] in [table] (optionally `schema.table`), from `information_schema.columns`;
         * [BINARY] when the table cannot be found (e.g. created later by Hibernate).
         */
        fun detect(dataSource: DataSource, table: String, column: String = "payload"): PayloadStorage {
            val (schema, name) = if ('.' in table) table.substringBefore('.') to table.substringAfter('.') else null to table
            val sql = "select data_type from information_schema.columns where upper(table_name) = upper(?) " +
                "and upper(column_name) = upper(?)" + (if (schema != null) " and upper(table_schema) = upper(?)" else "")
            val dataType = dataSource.connection.use { c ->
                c.prepareStatement(sql).use { st ->
                    st.setString(1, name)
                    st.setString(2, column)
                    if (schema != null) st.setString(3, schema)
                    st.executeQuery().use { rs -> if (rs.next()) rs.getString(1) else null }
                }
            } ?: return BINARY
            return fromTypeName(dataType)
        }

        /** Maps a database type name (PostgreSQL / H2 spelling) to its storage. */
        fun fromTypeName(dataType: String): PayloadStorage {
            val type = dataType.lowercase()
            return when {
                type == "oid" -> OID
                "char" in type || "text" in type || "clob" in type -> TEXT
                else -> BINARY
            }
        }
    }
}
