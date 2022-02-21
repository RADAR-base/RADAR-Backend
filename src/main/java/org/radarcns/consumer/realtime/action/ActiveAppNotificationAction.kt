package org.radarcns.consumer.realtime.action

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarbase.appserver.client.AppserverClient
import org.radarbase.appserver.client.AppserverClientConfig
import org.radarbase.appserver.client.MessagingType
import org.radarcns.config.realtime.ActionConfig
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

    @Throws(IllegalArgumentException::class, IOException::class)
    override fun executeFor(record: ConsumerRecord<*, *>?): Boolean {
        val key = record?.key() as GenericRecord

        require(key["projectId"] is String) { "Cannot execute Action $NAME. The projectId is not valid." }
        require(key["userId"] is String) { "Cannot execute Action $NAME. The userId is not valid." }
        require(key["sourceId"] is String) { "Cannot execute Action $NAME. The sourceId is not valid." }

        val project = key["projectId"] as String
        val user = key["userId"] as String
        val source = key["sourceId"] as String

        val timeStrategy: ScheduleTimeStrategy = if (timeOfDay != null && !timeOfDay.isEmpty()) {
            // get timezone for the user and create the correct local time of the day
            TimeOfDayStrategy(timeOfDay, getUserTimezone(project, user))
        } else {
            // no time of the day provided, schedule now.
            SimpleTimeStrategy(5, ChronoUnit.MINUTES)
        }

        // create the notification in appserver
        val contentProvider: NotificationContentProvider = ProtocolNotificationProvider(
                name = questionnaireName,
                scheduledTime = timeStrategy.scheduledTime,
                sourceId = source,
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

    init {
        questionnaireName = actionConfig.properties!!.getOrDefault("questionnaire_name", "ers") as String
        val appServerBaseUrl = actionConfig
                .properties
                .getOrDefault(
                        "appserver_base_url",
                        "https://radar-cns-platform.rosalind.kcl.ac.uk/appserver") as String
        timeOfDay = actionConfig.properties.getOrDefault("time_of_day", null) as String?
        val mpTokenUrl = actionConfig
                .properties
                .getOrDefault(
                        "management_portal_token_url",
                        "https://radar-cns-platform.rosalind.kcl.ac.uk/managementportal/api/ouath/token") as String
        type = MessagingType.valueOf(
                (actionConfig
                        .properties
                        .getOrDefault("message_type", MessagingType.NOTIFICATIONS.toString()) as String))
        val clientId = actionConfig.properties.getOrDefault("client_id", "realtime_consumer") as String
        val clientSecret = actionConfig.properties.getOrDefault("client_secret", "secret") as String
        val config = AppserverClientConfig()
        config.clientId = clientId
        config.clientSecret = clientSecret
        config.appserverUrl(appServerBaseUrl)
        config.tokenUrl(mpTokenUrl)
        appserverClient = AppserverClient(config)
    }
}