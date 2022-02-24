package org.radarcns.config.realtime

import com.fasterxml.jackson.annotation.JsonProperty

data class RealtimeConsumerConfig(
        val name: String,
        val topic: String,
        @JsonProperty("notify_errors")
        val notifyErrors: NotifyErrorConfig? = null,
        @JsonProperty("conditions")
        val conditionConfigs: List<ConditionConfig>,
        @JsonProperty("actions")
        val actionConfigs: List<ActionConfig>,
        @JsonProperty("consumer_properties")
        val consumerProperties: Map<String, String>? = null,
)

data class NotifyErrorConfig(
        @JsonProperty("email_addresses")
        val emailAddresses: List<String>? = null,
)