package org.radarcns.monitor.intervention

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.OkHttpClient
import org.radarbase.appconfig.client.AppConfigClient
import org.radarcns.config.monitor.AuthConfig
import org.radarcns.config.monitor.ThresholdAdaptationConfig

class ThresholdAdjustmentAlgorithm(
    private val clientId: String,
    private val state: InterventionMonitorState,
    private val config: ThresholdAdaptationConfig,
    appConfigUrl: String,
    authConfig: AuthConfig,
    httpClient: OkHttpClient,
    mapper: ObjectMapper,
) {
    private var appConfigClient: AppConfigClient<InterventionAppConfigState>

    init {
        appConfigClient = AppConfigClient(object : TypeReference<InterventionAppConfigState>() {}) {
            appConfigUrl(appConfigUrl)
            tokenUrl(authConfig.tokenUrl)
            clientId = authConfig.clientId
            clientSecret = authConfig.clientSecret
            this.httpClient = httpClient
            this.mapper = mapper
        }
    }

    fun updateThresholds() {
        state.counts.forEach { (userId, counts) ->
            val interventionConfig = appConfigClient.getUserConfig(counts.projectId, userId, clientId = clientId)
            interventionConfig.threshold = calculateThreshold(
                currentValue = interventionConfig.threshold,
                numberOfInterventions = counts.numberOfInterventions,
            )
            appConfigClient.setUserConfig(counts.projectId, userId, interventionConfig, clientId = clientId)
        }
    }

    private fun calculateThreshold(
        currentValue: Float,
        numberOfInterventions: Int
    ): Float {
        return if (numberOfInterventions > config.optimalInterventions) {
            currentValue - config.adjustValue
        } else if (numberOfInterventions < config.optimalInterventions) {
            currentValue + config.adjustValue
        } else {
            currentValue
        }
    }
}
