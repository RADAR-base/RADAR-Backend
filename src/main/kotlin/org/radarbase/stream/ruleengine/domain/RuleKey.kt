package org.radarbase.stream.ruleengine.domain

import com.fasterxml.jackson.annotation.JsonProperty

// TODO make the key agnostic for AppConfig
data class RuleKey(
    @JsonProperty("client_id") val clientId: String,
    @JsonProperty("scope") val scope: String,
    @JsonProperty("name") val name: String,
) {
    fun toStoreKey(): String = "$clientId|$scope|$name"
}
