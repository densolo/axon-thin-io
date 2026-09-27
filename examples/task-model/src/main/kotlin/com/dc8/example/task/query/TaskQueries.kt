package com.dc8.example.task.query

import com.dc8.example.task.api.TaskStatus
import com.dc8.example.task.domain.Comment
import com.dc8.example.task.domain.CommentRepository
import com.dc8.example.task.projection.TaskActivity
import com.dc8.example.task.projection.TaskActivityRepository
import com.dc8.example.task.projection.TaskSummary
import com.dc8.example.task.projection.TaskSummaryRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

/** Queries go straight to repositories — no QueryGateway / @QueryHandler involved. */
@Service
@Transactional(readOnly = true)
class TaskQueryService(
    private val summaries: TaskSummaryRepository,
    private val activities: TaskActivityRepository,
    private val comments: CommentRepository,
) {
    fun summary(taskId: String): TaskSummary? = summaries.findById(taskId).orElse(null)

    fun byStatus(status: TaskStatus): List<TaskSummary> = summaries.findByStatusOrderByTitle(status)

    fun activity(taskId: String): List<TaskActivity> = activities.findByTaskIdOrderById(taskId)

    fun comments(taskId: String): List<Comment> = comments.findByTaskIdOrderByCreatedAt(taskId)

    fun countTasks(): Long = summaries.count()
}
