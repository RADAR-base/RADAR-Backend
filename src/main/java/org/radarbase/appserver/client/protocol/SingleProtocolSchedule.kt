package org.radarbase.appserver.client.protocol

data class SingleProtocolSchedule(
    val clinicalProtocol: ClinicalProtocol? = null,
    val completionWindow: ProtocolDuration = ProtocolDuration(
        amount = 15,
        unit = "min",
    ),
    val notification: Notification = Notification(),
    val reminders: Reminders = Reminders(),
    val repeatProtocol: ProtocolDuration = ProtocolDuration(
        // a lot of years, it will not repeat.
        amount = 9999999999L,
        unit = "min",
    ),
    val repeatQuestionnaire: RepeatQuestionnaire? = null,
    val referenceTimestamp: String? = null,
)
