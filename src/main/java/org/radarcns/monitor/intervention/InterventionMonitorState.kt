package org.radarcns.monitor.intervention

import com.fasterxml.jackson.annotation.JsonIgnore
import java.time.Instant
import java.time.LocalDate
import java.time.Period
import java.time.ZoneOffset
import java.util.*

data class InterventionMonitorState(
    var fromDate: Instant = lastMidnight(),
    val counts: MutableMap<String, InterventionCount> = HashMap(),
    val exceptions: MutableMap<String, ProjectExceptions> = HashMap()
) {
    fun reset(fromDate: Instant) {
        this.fromDate = fromDate
        counts.clear()
        exceptions.clear()
    }

    operator fun get(intervention: InterventionRecord) = counts.computeIfAbsent(intervention.userId) {
        InterventionCount(intervention.projectId)
    }

    fun addException(intervention: InterventionRecord) {
        require(!intervention.exception.isNullOrEmpty()) { "Missing exception in intervention" }
        val userExceptions = exceptions
            .computeIfAbsent(intervention.projectId) { ProjectExceptions() }
            .exceptions
            .computeIfAbsent(intervention.userId) { UserExceptions() }
        userExceptions += intervention.exception
    }

    fun nextMidnight(): Instant = (LocalDate.ofInstant(fromDate, ZoneOffset.UTC) + ONE_DAY)
        .atStartOfDay(ZoneOffset.UTC)
        .toInstant()

    data class InterventionCount(
        val projectId: String,
        val interventions: MutableSet<Long> = mutableSetOf(),
    )

    data class ProjectExceptions(
        val exceptions: MutableMap<String, UserExceptions> = mutableMapOf(),
    )

    data class UserExceptions(
        var count: Int = 0,
        val lines: LinkedList<String> = LinkedList()
    ) {
        @get:JsonIgnore
        val isTruncated: Boolean
            get() = count > EXCEPTION_SIZE

        operator fun plusAssign(exception: String) {
            count += 1
            if (count > EXCEPTION_SIZE) lines.removeFirst()
            lines += exception
        }
    }

    companion object {
        fun lastMidnight(): Instant = LocalDate.now(ZoneOffset.UTC)
            .atStartOfDay(ZoneOffset.UTC)
            .toInstant()

        private val ONE_DAY = Period.ofDays(1)
        private const val EXCEPTION_SIZE = 3
    }
}
