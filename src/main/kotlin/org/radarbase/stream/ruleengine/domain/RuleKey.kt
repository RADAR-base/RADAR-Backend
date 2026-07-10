package org.radarbase.stream.ruleengine.domain

import com.fasterxml.jackson.annotation.JsonProperty

data class RuleKey(
    @JsonProperty("topic") val topicName: String,
    @JsonProperty("project") val project: String,
    @JsonProperty("condition") val condition: String,
)
