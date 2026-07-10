package org.radarbase.config

import com.fasterxml.jackson.annotation.JsonProperty

data class NotifyConfig(
    @param:JsonProperty("project_id")
    var projectId: String,

    @param:JsonProperty("email_address")
    var emailAddress: List<String>,
)
