package org.radarcns.monitor.intervention

import java.time.Instant

data class InterventionRecord(
    val projectId: String,
    val userId: String,
    val sourceId: String,
    val time: Long,
    val timeCompleted: Instant,
    val isFinal: Boolean,
    val decision: Boolean,
    val name: String?,
    val exception: String?,
) {
    companion object {
        fun Map<String, Any>.toInterventionRecord(userId: String): InterventionRecord {
            val projectId = requireNotNull(this["PROJECTID"] as? String) { "Cannot map record of $userId without project ID." }
            val intervention = requireNotNull(this["INTERVENTION"] as? Map<*, *>) { "Cannot map record without intervention for $projectId - $userId" }

            return InterventionRecord(
                projectId = projectId,
                userId = userId,
                sourceId = this["SOURCEID"] as String,
                time = (this["TIME"] as Number).toLong(),
                timeCompleted = Instant.ofEpochMilli(((this["TIMECOMPLETED"] as Number).toDouble() * 1000.0).toLong()),
                isFinal = this["ISFINAL"] as Boolean,
                decision = intervention["DECISION"] as Boolean,
                name = intervention["NAME"] as? String,
                exception = intervention["EXCEPTION"] as? String,
            )
        }
    }
}
