package org.radarbase.appserver.client.protocol


data class SingleProtocol(
    val name: String,
    val questionnaire: Questionnaire,
    val protocol: SingleProtocolSchedule,
    val showInCalendar: Boolean = true,
    val showIntroduction: Boolean = false,
    val type: String = "scheduled",
    val estimatedCompletionTime: Int = 1,
    val order: Int = 0,
    val isDemo: Boolean = false,
    val startText: MultiLingualText = mapOf(),
    val endText: MultiLingualText = mapOf(),
    val warn: MultiLingualText = mapOf(),
)
