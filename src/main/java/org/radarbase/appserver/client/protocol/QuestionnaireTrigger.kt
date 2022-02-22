package org.radarbase.appserver.client.protocol

data class QuestionnaireTrigger(
    val action: String = "QUESTIONNAIRE_TRIGGER",
    val questionnaire: SingleProtocol,
    val metadata: Map<String, String> = mapOf(),
)
