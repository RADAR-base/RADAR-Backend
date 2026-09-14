package org.radarbase.stream.ruleengine.domain

import com.fasterxml.jackson.annotation.JsonProperty
import org.radarbase.config.intervention.ActionConfig

data class AppConfigDatabaseRow(
    @JsonProperty("id") val id: Int,
    @JsonProperty("client_id") val clientId: String,
    val scope: String,
    val name: String,
    val value: ActionConfig,
    @JsonProperty("create_timestamp") val createTimestamp: Long,
    val version: Int,
)
