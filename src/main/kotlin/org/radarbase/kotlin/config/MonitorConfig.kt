package org.radarbase.kotlin.config

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * POJO representing a monitor configuration
 */
class MonitorConfig {
    @JsonProperty("notify")
    var notifyConfig: List<NotifyConfig> = emptyList()

    @JsonProperty("email_host")
    var emailHost: String = ""

    @JsonProperty("email_port")
    var emailPort: Int = 0

    @JsonProperty("email_user")
    var emailUser: String = ""

    @JsonProperty("log_interval")
    var logInterval: Int = 1000

    var topics: List<String>? = null

    @JsonProperty("message")
    var message: String? = null
}