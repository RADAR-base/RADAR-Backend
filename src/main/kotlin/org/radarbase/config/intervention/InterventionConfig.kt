package org.radarbase.config.intervention

import com.fasterxml.jackson.annotation.JsonProperty

data class InterventionConfig(
    val name: String?,
    val topic: String,
    @JsonProperty("conditions")
    val conditionConfigs: List<ConditionConfig>,
    @JsonProperty("actions")
    val actionConfigs: List<ActionConfig>,
    @JsonProperty("consumer_properties")
    val consumerProperties: Map<String, String>? = null,
)
