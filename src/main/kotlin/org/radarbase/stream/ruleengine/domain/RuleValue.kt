package org.radarbase.stream.ruleengine.domain

import com.fasterxml.jackson.annotation.JsonProperty

data class RuleValue(
    @JsonProperty("action_configs") val actionConfigs: List<ActionConfig>,
)
