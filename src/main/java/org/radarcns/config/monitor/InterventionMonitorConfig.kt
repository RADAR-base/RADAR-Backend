package org.radarcns.config.monitor

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Duration

data class InterventionMonitorConfig(
    @JsonProperty("app_server_url")
    val appServerUrl: String,
    @JsonProperty("app_config_client")
    val ksqlAppConfigClient: String,
    @JsonProperty("app_config_url")
    val appConfigUrl: String,
    @JsonProperty("auth")
    val authConfig: AuthConfig,

    @JsonProperty("notify")
    val emailNotifyConfig: List<EmailNotifyConfig> = listOf(),

    // The list of intervention topics, which will be used to evaluate the conditions
    val topics: List<String>,

    // List of notification configs and corresponding conditions to trigger
    @JsonProperty("ttl_margin")
    val ttlMargin: Duration = Duration.ofMinutes(5),
    val properties: Map<String, String> = mapOf(),
    val deadline: Duration = Duration.ofMinutes(15),
    @JsonProperty("state_reset_interval")
    val stateResetInterval: Duration = Duration.ofHours(24),

    @JsonProperty("threshold_adaptation")
    val thresholdAdaptation: ThresholdAdaptationConfig = ThresholdAdaptationConfig(),

    @JsonProperty("max_interventions")
    val maxInterventions: Int = 4,

    @JsonProperty("protocol_directory")
    val protocolDirectory: String,
    val defaultLanguage: String = "en",
) {
    fun withEnv(): InterventionMonitorConfig = copy(authConfig = authConfig.withEnv())
}
