package org.radarcns.consumer.realtime.action

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JacksonException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarbase.appserver.client.AppserverClient
import org.radarbase.appserver.client.AppserverClientConfig
import org.radarcns.config.realtime.ActionConfig
import org.radarcns.consumer.realtime.Grouping.Companion.objectMapper
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.time.*

/**
 * This action can be used to trigger a notification for the aRMT app and schedule a corresponding
 * questionnaire for the user to fill out. This can also work as an intervention mechanism in some
 * use-cases.
 */
class AppServerTriggerAction(
    actionConfig: ActionConfig,
    override val name: String = NAME,
) : ActionBase(actionConfig) {
    private val parsedConfig: AppServerTriggerActionConfig =
        AppServerTriggerActionConfig.fromMap(actionConfig.properties)
    private val appserverClient: AppserverClient = AppserverClient(parsedConfig.appserverClientConfig)


    @Throws(IllegalArgumentException::class, IOException::class)
    override fun executeFor(record: ConsumerRecord<*, *>?): Boolean {

        logger.debug("Executing action $name for record: $record")

        val key = getKeys(record) ?: return false

        if (getTime(record) < Instant.now().minus(Duration.ofDays(parsedConfig.toleranceInDays.toLong()))
                .toEpochMilli()
        ) {
            logger.info("Skipping notification for ${key.userId} because it is too late.")
            return false
        }

        appserverClient.createScheduleForAssessment(
            projectId = key.projectId, userId = key.userId, body = parsedConfig.assessment.toString()
        )

        logger.info("Scheduled task on the appserver for ${key.userId}")
        return true
    }

    companion object {
        const val NAME = "AppServerTriggerAction"
        private val logger = LoggerFactory.getLogger(AppServerTriggerAction::class.java)
    }
}

data class AppServerTriggerActionConfig(
    @JsonProperty("appserver_base_url") val appServerBaseUrl: String,
    @JsonProperty("management_portal_token_url") val mpTokenUrl: String,
    @JsonProperty("assessment_json") val assessmentJson: String?,
    @JsonProperty("assessment_json_file") val assessmentJsonFile: String?,
    @JsonProperty("client_id") val clientId: String = "realtime_consumer",
    @JsonProperty("client_secret") val clientSecret: String = "secret",
    @JsonProperty("assessment_read_method") val method: Methods = Methods.JSON_INLINE,
    @JsonProperty("tolerance_in_days") val toleranceInDays: Int = 5,
    @JsonIgnore val appserverClientConfig: AppserverClientConfig = AppserverClientConfig(
        clientId = clientId,
        clientSecret = clientSecret,
    ).apply { appserverUrl(appServerBaseUrl); tokenUrl(mpTokenUrl); this.mapper = objectMapper }
) {

    @JsonIgnore
    val assessment: JsonNode = when (method) {
        Methods.JSON_INLINE -> {
            if (assessmentJson.isNullOrEmpty()) {
                throw IllegalArgumentException("assessment_json is required with method ${Methods.JSON_INLINE}")
            }
            validateJson(assessmentJson)
        }
        Methods.JSON_FILE -> {
            if (assessmentJsonFile.isNullOrEmpty()) {
                throw IllegalArgumentException("assessment_json_file is required with method ${Methods.JSON_FILE}")
            }
            validateJson(File(assessmentJsonFile).inputStream().readBytes().toString(Charsets.UTF_8))
        }
    }

    private fun validateJson(json: String): JsonNode {
        val node = try {
            ObjectMapper().readTree(json)
        } catch (e: JacksonException) {
            throw IllegalArgumentException("Invalid JSON format.", e)
        }

        if (!node.isObject) {
            throw IllegalArgumentException("The provided assessment is not valid JSON format.")
        }

        return node
    }

    companion object {

        fun fromMap(properties: Map<String, Any>?): AppServerTriggerActionConfig {
            return properties?.let { map ->

                AppServerTriggerActionConfig(
                    appServerBaseUrl = requireNotNull(map["appserver_base_url"]) as String,
                    mpTokenUrl = requireNotNull(map["management_portal_token_url"]) as String,
                    assessmentJson = map["assessment_json"] as String?,
                    assessmentJsonFile = map["assessment_json_file"] as String?,
                    clientId = map["client_id"] as String? ?: "realtime_consumer",
                    clientSecret = map["client_secret"] as String? ?: "secret",
                    method = Methods.valueOf(map["assessment_read_method"] as String? ?: "JSON_INLINE"),
                    toleranceInDays = map["tolerance_in_days"] as Int? ?: 5,
                )
            } ?: throw IllegalArgumentException("Missing required properties")
        }
    }
}

enum class Methods {
    JSON_INLINE, JSON_FILE
}