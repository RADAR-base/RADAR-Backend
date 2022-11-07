package org.radarcns.config.monitor

import com.fasterxml.jackson.annotation.JsonProperty

data class AuthConfig(
    @JsonProperty("clientId")
    val clientId: String? = null,
    @JsonProperty("clientSecret")
    val clientSecret: String? = null,
    @JsonProperty("token_url")
    val tokenUrl: String,
) {
    fun withEnv(): AuthConfig {
        var result = this;
        val envClientId = System.getenv("AUTH_CLIENT_ID")
        if (envClientId != null) {
            result = result.copy(clientId = envClientId)
        }
        val envClientSecret = System.getenv("AUTH_CLIENT_SECRET")
        if (envClientSecret != null) {
            result = result.copy(clientSecret = envClientSecret)
        }
        return result
    }
}
