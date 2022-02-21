package org.radarcns.config.realtime

import com.fasterxml.jackson.annotation.JsonProperty

data class RealtimeConsumerConfig(
        val name: String,
        val topic: String,
        @JsonProperty("conditions")
        val conditionConfigs: List<ConditionConfig>,
        @JsonProperty("actions")
        val actionConfigs: List<ActionConfig>,
        @JsonProperty("consumer_properties")
        val consumerProperties: Map<String, String>? = null,
)