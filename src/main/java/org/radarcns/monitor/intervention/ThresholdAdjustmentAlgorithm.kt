package org.radarcns.monitor.intervention

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.OkHttpClient
import org.radarbase.appconfig.client.AppConfigClient
import org.radarcns.config.monitor.AuthConfig
import org.radarcns.config.monitor.ThresholdAdaptationConfig
import org.slf4j.LoggerFactory

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
            try {
                val interventionConfig = appConfigClient.getUserConfig(
                    projectId = counts.projectId,
                    userId = userId,
                    clientId = clientId,
                )
                val newInterventionConfig = interventionConfig.copy(
                    threshold = calculateThreshold(
                        currentValue = interventionConfig.threshold,
                        numberOfInterventions = counts.interventions.size,
                    )
                )
                logger.info(
                    "Updating thresholds for user {} from {} to {}",
                    userId, interventionConfig, newInterventionConfig
                )
                appConfigClient.setUserConfig(
                    projectId = counts.projectId,
                    userId = userId,
                    config = newInterventionConfig,
                    clientId = clientId,
                )
            } catch (ex: Throwable) {
                logger.error("Failed to update app config for {} - {}: {}",
                    counts.projectId, userId, ex.toString())
            }
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

    companion object {
        private val logger = LoggerFactory.getLogger(ThresholdAdjustmentAlgorithm::class.java)
    }
}
