package org.radarcns.monitor.intervention

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

data class RawInterventionRecord(
    @JsonProperty("PROJECTID")
    val projectId: String,
    @JsonProperty("SOURCEID")
    val sourceId: String,
    @JsonProperty("TIME")
    val time: Double,
    @JsonProperty("TIMECOMPLETED")
    val timeCompleted: Double,
    @JsonProperty("ISFINAL")
    val isFinal: Boolean,
    @JsonProperty("INTERVENTION")
    val intervention: RawInterventionDecisionRecord,
) {
    fun toInterventionRecord(userId: String): InterventionRecord = InterventionRecord(
        projectId = projectId,
        userId = userId,
        sourceId = sourceId,
        time = time.toLong(),
        timeCompleted = Instant.ofEpochMilli((timeCompleted * 1000.0).toLong()),
        isFinal = isFinal,
        decision = intervention.decision,
        name = intervention.name,
        exception = intervention.exception,
    )
}

