package org.radarcns.consumer.realtime.action

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarbase.appserver.client.AppserverClient
import org.radarbase.appserver.client.AppserverClientConfig
import org.radarbase.appserver.client.MessagingType
import org.radarcns.config.realtime.ActionConfig
import org.radarcns.consumer.realtime.Grouping.Companion.objectMapper
import org.radarcns.consumer.realtime.action.appserver.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * This action can be used to trigger a notification for the aRMT app and schedule a corresponding
 * questionnaire for the user to fill out. This can also work as an intervention mechanism in some
 * use-cases.
 */
class ActiveAppNotificationAction(
        actionConfig: ActionConfig,
        override val name: String = NAME,
) : ActionBase(actionConfig) {
    private val parsedConfig: ActiveAppNotificationActionConfig
    private val appserverClient: AppserverClient


    init {
        parsedConfig = ActiveAppNotificationActionConfig.fromMap(actionConfig.properties)
        appserverClient = AppserverClient(parsedConfig.appserverClientConfig)
    }

    @Throws(IllegalArgumentException::class, IOException::class)
    override fun executeFor(record: ConsumerRecord<*, *>?): Boolean {

        logger.debug("Executing action for record: $record")

        val key = getKeys(record) ?: return false

        val timezone = getUserTimezone(key.projectId, key.userId)
        logger.debug("Got user timezone")

        val timeStrategy: ScheduleTimeStrategy = if (!parsedConfig.timeOfDay.isNullOrEmpty()) {
            // get timezone for the user and create the correct local time of the day
            TimeOfDayStrategy(parsedConfig.timeOfDay, timezone)
        } else {
            // no time of the day provided, schedule now.
            SimpleTimeStrategy(parsedConfig.optionalDelayMinutes.toLong(), ChronoUnit.MINUTES)
        }

        val metadata = try {
            if (!parsedConfig.metadataKey.isNullOrEmpty()) {
                val root = ObjectMapper()
                        .readTree(
                                (record?.value() as GenericRecord?)
                                        ?.get(parsedConfig.metadataKey).toString()
                        )
                if (root.isArray) {
                    root.mapIndexed { k1, v1 ->
                        Pair("metadata-$k1", objectMapper.writeValueAsString(v1))
                    }.toMap()
                } else {
                    objectMapper.convertValue(root, object : TypeReference<Map<String, String?>>() {})
                }
            } else {
                null
            }
        } catch (e: Exception) {
            logger.error("Failed to parse metadata", e)
            null
        }

        logger.debug("Parsed metadata")


        if (getTime(record) < Instant.now()
                        .minus(Duration.ofDays(parsedConfig.toleranceInDays.toLong()))
                        .toEpochMilli()
        ) {
            logger.info("Skipping notification for ${key.userId} because it is too late.")
            return false
        }

        logger.debug("Tolerance is ok")

        // create the notification in appserver
        val contentProvider: NotificationContentProvider = ProtocolNotificationProvider(
                name = parsedConfig.questionnaireName,
                scheduledTime = timeStrategy.scheduledTime,
                sourceId = key.sourceId,
                metadata = metadata ?: emptyMap(),
                userTimezone = timezone
        )

        logger.debug("Sending message to appserver: ${key.projectId}, ${key.userId}, ${parsedConfig.type}")

        when (parsedConfig.type) {
            MessagingType.NOTIFICATIONS -> appserverClient.createMessage(
                    key.projectId, key.userId, MessagingType.NOTIFICATIONS, contentProvider.notificationMessage
            )
            MessagingType.DATA -> appserverClient.createMessage(
                    key.projectId, key.userId, MessagingType.DATA, contentProvider.dataMessage
            )
            MessagingType.ALL -> {
                appserverClient.createMessage(key.projectId, key.userId, MessagingType.NOTIFICATIONS, contentProvider.notificationMessage)
                appserverClient.createMessage(key.projectId, key.userId, MessagingType.DATA, contentProvider.dataMessage)
            }
        }
        logger.info("Sent notification to appserver for ${key.userId}")
        return true
    }

    @Throws(IOException::class)
    private fun getUserTimezone(project: String, user: String): String {
        return (appserverClient.getUserDetails(project, user)["timezone"]
                ?: parsedConfig.defaultTimeZone) as String
    }

    companion object {
        const val NAME = "ActiveAppNotificationAction"
        private val logger = LoggerFactory.getLogger(ActiveAppNotificationAction::class.java)
    }
}

data class ActiveAppNotificationActionConfig(
        @JsonProperty("appserver_base_url")
        val appServerBaseUrl: String,
        @JsonProperty("questionnaire_name")
        val questionnaireName: String,
        @JsonProperty("management_portal_token_url")
        val mpTokenUrl: String,
        @JsonProperty("message_type")
        val type: MessagingType = MessagingType.NOTIFICATIONS,
        @JsonProperty("client_id")
        val clientId: String = "realtime_consumer",
        @JsonProperty("client_secret")
        val clientSecret: String = "secret",
        @JsonProperty("time_of_day")
        val timeOfDay: String? = null,
        @JsonProperty("default_timezone")
        val defaultTimeZone: String = "UTC",
        @JsonProperty("metadata_key")
        val metadataKey: String? = null,
        @JsonProperty("tolerance_in_days")
        val toleranceInDays: Int = 5,
        @JsonProperty("optional_delay_minutes")
        val optionalDelayMinutes: Int = 5,
        @JsonIgnore
        val appserverClientConfig: AppserverClientConfig = AppserverClientConfig(
                clientId = clientId,
                clientSecret = clientSecret,
        ).apply { appserverUrl(appServerBaseUrl); tokenUrl(mpTokenUrl); this.mapper = objectMapper }
) {
    init {
        if (timeOfDay != null && !timeOfDay.matches("[0-9]{2}:[0-9]{2}:[0-9]{2}".toRegex())) {
            throw IllegalArgumentException("timeOfDay must be in the format HH:mm:ss")
        }
    }

    companion object {

        fun fromMap(properties: Map<String, Any>?): ActiveAppNotificationActionConfig {
            return properties?.let { map ->

                ActiveAppNotificationActionConfig(
                        appServerBaseUrl = requireNotNull(map["appserver_base_url"]) as String,
                        questionnaireName = requireNotNull(map["questionnaire_name"]) as String,
                        mpTokenUrl = requireNotNull(map["management_portal_token_url"]) as String,
                        type = MessagingType.valueOf(map["message_type"] as String?
                                ?: "NOTIFICATIONS"),
                        clientId = map["client_id"] as String? ?: "realtime_consumer",
                        clientSecret = map["client_secret"] as String? ?: "secret",
                        timeOfDay = map["time_of_day"] as String?,
                        defaultTimeZone = map["default_timezone"] as String? ?: "UTC",
                        metadataKey = map["metadata_key"] as String?,
                        toleranceInDays = map["tolerance_in_days"] as Int? ?: 5,
                        optionalDelayMinutes = map["optional_delay_minutes"] as Int? ?: 5,
                )
            } ?: throw IllegalArgumentException("Missing required properties")
        }
    }
}
