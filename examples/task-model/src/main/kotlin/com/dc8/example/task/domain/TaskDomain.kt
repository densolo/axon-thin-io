package com.dc8.example.task.domain

import com.dc8.example.task.api.TaskStatus
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EnumType
import jakarta.persistence.Enumerated
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.Version
import org.springframework.data.jpa.repository.JpaRepository
import java.time.Instant

/** Write model: state-stored entities mutated by command handlers (no aggregates / event sourcing). */
@Entity
@Table(name = "task")
class Task(
    @Id val id: String,
    var title: String,
    @Column(length = 4000) var description: String?,
    @Enumerated(EnumType.STRING) var status: TaskStatus = TaskStatus.TODO,
    var assignee: String? = null,
    val createdAt: Instant,
    val createdBy: String?,
) {
    @Version
    var version: Long = 0
}

@Entity
@Table(name = "task_comment")
class Comment(
    @Id val id: String,
    val taskId: String,
    val author: String,
    @Column(length = 4000) var text: String,
    val createdAt: Instant,
) {
    @Version
    var version: Long = 0
}

interface TaskRepository : JpaRepository<Task, String>

interface CommentRepository : JpaRepository<Comment, String> {
    fun findByTaskIdOrderByCreatedAt(taskId: String): List<Comment>
}
