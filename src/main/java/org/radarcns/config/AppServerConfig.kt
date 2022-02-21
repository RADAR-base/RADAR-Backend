package org.radarcns.config

import com.fasterxml.jackson.annotation.JsonProperty

data class AppServerConfig(
        @JsonProperty("base_url")
        val baseUrl: String,
        @JsonProperty("token_url")
        val tokenUrl: String,
        @JsonProperty("client_id")
        var clientId: String? = null,
        @JsonProperty("client_secret")
        var clientSecret: String? = null,
) {
    fun withEnv(): AppServerConfig {
        val envClientId = System.getenv("APP_SERVER_CLIENT_ID")
        if (envClientId != null) {
            clientId = envClientId
        }
        val envClientSecret = System.getenv("APP_SERVER_CLIENT_SECRET")
        if (envClientSecret != null) {
            clientSecret = envClientSecret
        }
        return this
    }
}