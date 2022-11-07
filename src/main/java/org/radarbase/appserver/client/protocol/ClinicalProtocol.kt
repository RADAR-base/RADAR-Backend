package org.radarbase.appserver.client.protocol

data class ClinicalProtocol(
    val repeatAfterClinicVisit: RepeatQuestionnaire,
    val requiresInClinicCompletion: Boolean = true,
)
