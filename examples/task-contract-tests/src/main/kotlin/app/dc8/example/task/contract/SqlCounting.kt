package app.dc8.example.task.contract

import net.ttddyy.dsproxy.ExecutionInfo
import net.ttddyy.dsproxy.QueryInfo
import net.ttddyy.dsproxy.listener.QueryExecutionListener
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.context.annotation.Bean
import java.util.concurrent.CopyOnWriteArrayList
import javax.sql.DataSource

/** Every statement the application sends, with its JDBC batch size. */
class SqlRecorder : QueryExecutionListener {
    private val executions = CopyOnWriteArrayList<Pair<String, Int>>()

    override fun beforeQuery(execInfo: ExecutionInfo, queryInfoList: List<QueryInfo>) = Unit

    override fun afterQuery(execInfo: ExecutionInfo, queryInfoList: List<QueryInfo>) {
        queryInfoList.forEach { executions += it.query.trim().lowercase() to maxOf(1, execInfo.batchSize) }
    }

    fun clear() = executions.clear()

    /** Round trips whose SQL matches. */
    fun count(match: (String) -> Boolean): Int = executions.count { match(it.first) }

    /** Rows sent by matching statements (JDBC batch size). */
    fun rows(match: (String) -> Boolean): Int = executions.filter { match(it.first) }.sumOf { it.second }
}

/**
 * `@Import(SqlCountingConfiguration::class)` in a test: wraps the `dataSource` bean so every statement is recorded in
 * the [SqlRecorder] bean. Not a component-scanned configuration — only tests that import it are affected.
 */
class SqlCountingConfiguration {

    @Bean
    fun sqlRecorder() = SqlRecorder()

    companion object {
        @JvmStatic
        @Bean
        fun countingDataSource(recorder: ObjectProvider<SqlRecorder>): BeanPostProcessor = object : BeanPostProcessor {
            override fun postProcessAfterInitialization(bean: Any, beanName: String): Any =
                if (bean is DataSource && beanName == "dataSource") {
                    ProxyDataSourceBuilder.create(bean).listener(object : QueryExecutionListener {
                        override fun beforeQuery(e: ExecutionInfo, q: List<QueryInfo>) = Unit
                        override fun afterQuery(e: ExecutionInfo, q: List<QueryInfo>) = recorder.getObject().afterQuery(e, q)
                    }).build()
                } else bean
        }
    }
}
