package org.radarcns.consumer.realtime.action

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarbase.appserver.client.AppserverClient
import org.radarbase.appserver.client.AppserverClientConfig
import org.radarbase.appserver.client.MessagingType
import org.radarcns.config.realtime.ActionConfig
import org.radarcns.consumer.realtime.Grouping.Companion.PROJECT_ID_KEYS
import org.radarcns.consumer.realtime.Grouping.Companion.SOURCE_ID_KEYS
import org.radarcns.consumer.realtime.Grouping.Companion.USER_ID_KEYS
import org.radarcns.consumer.realtime.Grouping.Companion.findKey
import org.radarcns.consumer.realtime.action.appserver.*
import java.io.IOException
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
    private val questionnaireName: String
    private val timeOfDay: String?
    private val appserverClient: AppserverClient
    private val type: MessagingType
    private val metadataKey: String?


    init {
        questionnaireName = requireNotNull(actionConfig
                .properties!!
                .getOrDefault(
                        "questionnaire_name",
                        null
                ) as String?) { "Questionnaire name is required" }

        val appServerBaseUrl = requireNotNull(actionConfig
                .properties
                .getOrDefault(
                        "appserver_base_url",
                        null
                ) as String?) { "Appserver base url is required" }

        timeOfDay = actionConfig.properties.getOrDefault("time_of_day", null) as String?
        val mpTokenUrl = actionConfig
                .properties
                .getOrDefault(
                        "management_portal_token_url",
                        null) as String?
        type = MessagingType.valueOf(
                (actionConfig
                        .properties
                        .getOrDefault("message_type", MessagingType.NOTIFICATIONS.toString()) as String))

        val clientId = actionConfig.properties.getOrDefault("client_id", "realtime_consumer") as String
        val clientSecret = actionConfig.properties.getOrDefault("client_secret", "secret") as String
        val config = AppserverClientConfig(
                clientId = clientId,
                clientSecret = clientSecret,
        )
        config.appserverUrl(appServerBaseUrl)
        config.tokenUrl(mpTokenUrl)
        appserverClient = AppserverClient(config)

        metadataKey = actionConfig
                .properties
                .getOrDefault(
                        "metadata_key",
                        null) as String?
    }

    @Throws(IllegalArgumentException::class, IOException::class)
    override fun executeFor(record: ConsumerRecord<*, *>?): Boolean {
        val key = record?.key() as GenericRecord

        val pidKey: String = findKey(record, PROJECT_ID_KEYS)
                ?: throw IllegalArgumentException("No project id found in key")

        val uidKey: String = findKey(record, USER_ID_KEYS)
                ?: throw IllegalArgumentException("No user id found in key")

        val sidKey: String? = findKey(record, SOURCE_ID_KEYS)

        require(key[pidKey] is String) { "Cannot execute Action $NAME. The projectId is not valid." }
        require(key[uidKey] is String) { "Cannot execute Action $NAME. The userId is not valid." }

        val project = key[pidKey] as String
        val user = key[uidKey] as String
        val source = if (sidKey != null) key[sidKey] as String else null

        val timeStrategy: ScheduleTimeStrategy = if (!timeOfDay.isNullOrEmpty()) {
            // get timezone for the user and create the correct local time of the day
            TimeOfDayStrategy(timeOfDay, getUserTimezone(project, user))
        } else {
            // no time of the day provided, schedule now.
            SimpleTimeStrategy(5, ChronoUnit.MINUTES)
        }

        val metadata = ObjectMapper()
                .readValue(
                        (record.value() as GenericRecord?)?.get(metadataKey) as String,
                        object : TypeReference<Map<String, String>?>() {}
                )

        // create the notification in appserver
        val contentProvider: NotificationContentProvider = ProtocolNotificationProvider(
                name = questionnaireName,
                scheduledTime = timeStrategy.scheduledTime,
                sourceId = source,
                metadata = metadata ?: emptyMap(),
        )

        val body = when (type) {
            MessagingType.NOTIFICATIONS -> contentProvider.notificationMessage
            MessagingType.DATA -> contentProvider.dataMessage
        }
        appserverClient.createMessage(project, user, type, body)
        return true
    }

    @Throws(IOException::class)
    private fun getUserTimezone(project: String, user: String): String {
        return (appserverClient.getUserDetails(project, user)["timezone"] ?: "gmt") as String
    }

    companion object {
        const val NAME = "ActiveAppNotificationAction"
    }
}