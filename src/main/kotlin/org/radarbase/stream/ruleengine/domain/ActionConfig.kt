package org.radarbase.stream.ruleengine.domain

import com.fasterxml.jackson.annotation.JsonProperty

data class ActionConfig(
    @JsonProperty("topic") val topic: String,
    @JsonProperty("action_attributes") val actionAttributes: Map<String, Any>,
)
