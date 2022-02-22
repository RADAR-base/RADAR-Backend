package org.radarbase.appserver.client.protocol

import com.fasterxml.jackson.annotation.JsonIgnore
import org.radarcns.consumer.realtime.Grouping.Companion.objectMapper

data class QuestionnaireTrigger(
        @JsonIgnore
        val singleProtocol: SingleProtocol,
        @JsonIgnore
        val metadataMap: Map<String, String?> = mapOf(),

        val action: String = "QUESTIONNAIRE_TRIGGER",
        val questionnaire: String = objectMapper.writeValueAsString(singleProtocol),
        val metadata: String = objectMapper.writeValueAsString(metadataMap),
)
