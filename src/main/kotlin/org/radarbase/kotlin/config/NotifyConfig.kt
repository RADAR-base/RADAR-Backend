package org.radarbase.kotlin.config

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * POJO to store each email Notification configuration.
 */
class NotifyConfig @JsonCreator constructor(
    @JsonProperty("project_id") var projectId: String,
    @JsonProperty("email_address") var emailAddress: List<String>
)
