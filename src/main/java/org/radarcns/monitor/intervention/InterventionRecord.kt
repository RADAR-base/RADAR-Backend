package org.radarcns.monitor.intervention

import com.fasterxml.jackson.databind.JsonNode
import java.time.Instant

data class InterventionRecord(
    val projectId: String,
    val userId: String,
    val sourceId: String,
    val time: Instant,
    val timeNotification: Long,
    val isFinal: Boolean,
    val decision: Boolean,
    val name: String,
    val exception: String,
) {
    companion object {
        fun JsonNode.toInterventionRecord(userId: String): InterventionRecord {
            val projectId = this["PROJECTID"].asText("")

            if (!has("INTERVENTION")) {
                throw IllegalArgumentException("Cannot map record without intervention for $projectId - $userId")
            }
            val intervention = this["INTERVENTION"]

            return InterventionRecord(
                projectId = projectId,
                userId = userId,
                sourceId = this["SOURCEID"].asText(""),
                time = Instant.ofEpochMilli((intervention["TIME"].asDouble() * 1000.0).toLong()),
                timeNotification = this["TIMENOTIFICATION"].asDouble().toLong(),
                isFinal = this["ISFINAL"].asBoolean(),
                decision = intervention["DECISION"].asBoolean(),
                name = intervention["NAME"].asText(""),
                exception = intervention["EXCEPTION"].asText(""),
            )
        }
    }
}
