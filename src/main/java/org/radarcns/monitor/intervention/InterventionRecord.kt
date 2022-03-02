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
)
