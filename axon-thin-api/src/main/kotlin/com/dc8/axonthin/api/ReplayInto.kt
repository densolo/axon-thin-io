package com.dc8.axonthin.api

import kotlin.reflect.KClass

/**
 * Marks an event-handling bean as a projection that thin's `ProjectionMigrator.migrate()` may rebuild, and names the
 * JPA entities it writes.
 *
 * `migrate()` replays the event store into the projection when **all** its entities are empty and the store holds
 * events it handles. Resetting a projection (for a rebuild or another branch) is done outside thin — e.g. a
 * Liquibase change that empties or renames the table; the next `migrate()` fills it again.
 *
 * Entity classes rather than table names: renaming a table (`@Table(name = "task_summary_v3")`) needs no change here.
 * Beans without this annotation are never replayed (e.g. handlers with side effects).
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class ReplayInto(vararg val value: KClass<*>)
