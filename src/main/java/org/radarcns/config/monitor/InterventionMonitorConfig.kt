package org.radarcns.config.monitor

import com.fasterxml.jackson.annotation.JsonProperty
import org.radarcns.config.EmailServerConfig
import java.time.Duration

data class InterventionMonitorConfig(
    @JsonProperty("app_server_url")
    val appServerUrl: String,
    @JsonProperty("ksql_app_config_client")
    val ksqlAppConfigClient: String,
    @JsonProperty("app_config_url")
    val appConfigUrl: String,
    @JsonProperty("auth")
    val authConfig: AuthConfig,

    @JsonProperty("notify")
    var emailNotifyConfig: List<EmailNotifyConfig> = listOf(),
    @JsonProperty("email_server")
    val emailServerConfig: EmailServerConfig?,

    // The list of intervention topics, which will be used to evaluate the conditions
    var topic: String,

    // List of notification configs and corresponding conditions to trigger
    var ttlMargin: Duration = Duration.ofMinutes(5),
    var properties: Map<String, String> = mapOf(),
    var deadline: Duration = Duration.ofMinutes(15),

    @JsonProperty("threshold_adaptation")
    var thresholdAdaptation: ThresholdAdaptationConfig = ThresholdAdaptationConfig(),

    @JsonProperty("max_interventions")
    var maxInterventions: Int = 4,

    val protocolDirectory: String,
    val defaultLanguage: String = "en",
) {

    fun withEnv(): InterventionMonitorConfig = copy(authConfig = authConfig.withEnv())
}
