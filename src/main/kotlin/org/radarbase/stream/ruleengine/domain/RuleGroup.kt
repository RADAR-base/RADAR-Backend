package org.radarbase.stream.ruleengine.domain

import com.fasterxml.jackson.annotation.JsonProperty

data class RuleGroup(
    @JsonProperty("rules") val rules: List<Pair<RuleKey, RuleValue>> = emptyList(),
)
