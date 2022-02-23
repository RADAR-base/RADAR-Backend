package org.radarcns.monitor.intervention

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.OkHttpClient
import org.radarbase.appserver.client.AppServerDataMessage
import org.radarbase.appserver.client.AppServerNotification
import org.radarbase.appserver.client.AppserverClient
import org.radarbase.appserver.client.MessagingType
import org.radarbase.appserver.client.protocol.*
import org.radarcns.config.monitor.AuthConfig
import org.radarbase.appserver.client.protocol.Notification.Companion.defaultNotificationText
import org.radarbase.appserver.client.protocol.Notification.Companion.defaultNotificationTitle
import org.radarcns.monitor.intervention.InterventionMonitor.Companion.passedDuration
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

class AppServerIntervention(
    private val protocolDirectory: ProtocolDirectory,
    appserverUrl: String,
    private val defaultLanguage: String,
    private val cacheDuration: Duration,
    authConfig: AuthConfig,
    httpClient: OkHttpClient,
    mapper: ObjectMapper,
) {
    private val appserverClient: AppserverClient
    private val questionnaireWriter = mapper.writerFor(SingleProtocol::class.java)
    private val notificationWriter = mapper.writerFor(AppServerNotification::class.java)
    private val dataWriter = mapper.writerFor(AppServerDataMessage::class.java)
    private val mapWriter = mapper.writerFor(object : TypeReference<Map<String, String>>() {})
    private val userDetailCache: MutableMap<String, UserDetailCache> = mutableMapOf()
    private val dateTimeFormatter = DateTimeFormatter.ofPattern("dd-MM-YYYY:HH:mm")

    init {
        appserverClient = AppserverClient {
            appserverUrl(appserverUrl)
            tokenUrl(authConfig.tokenUrl)
            clientId = authConfig.clientId
            clientSecret = authConfig.clientSecret
            this.httpClient = httpClient
            this.mapper = mapper
        }
    }

    fun createMessages(intervention: InterventionRecord, ttl: Duration) {
        val attributes = if (intervention.name != null) mapOf("intervention" to intervention.name) else emptyMap()
        val protocol = protocolDirectory.get(
            projectId = intervention.projectId,
            userId = intervention.userId,
            attributes = attributes,
        ) ?: throw NoSuchElementException("No protocol found for $intervention")

        val userDetails = getUserDetails(intervention)
        val questionnaire = protocol.questionnaire.copy(
            protocol = protocol.questionnaire.protocol.adjustTimestamp(userDetails),
        )
        val dataMap =  mapOf(
            "action" to protocol.action,
            "metadata" to mapWriter.writeValueAsString(protocol.metadata),
            "questionnaire" to questionnaireWriter.writeValueAsString(questionnaire),
        )

        val ttlSeconds = ttl.toSeconds()
        val notificationResponse = createNotificationMessage(intervention, ttlSeconds, protocol, userDetails, dataMap)
        val dataResponse = createDataMessage(intervention, ttlSeconds, dataMap)

        logger.debug("Created App Server message for notification {} and data {}",
            notificationResponse, dataResponse)
    }

    private fun getUserDetails(intervention: InterventionRecord): UserDetailCache {
        val cache = userDetailCache[intervention.userId]
        return if (cache == null || cache.fetchedAt.passedDuration() > cacheDuration) {
            val userDetails = appserverClient.getUserDetails(intervention.projectId, intervention.userId)
            UserDetailCache(
                language = userDetails["language"] as? String,
                zoneId = (userDetails["timezone"] as? String)?.let { timezone ->
                    try {
                        ZoneId.of(timezone)
                    } catch (ex: Exception) {
                        null
                    }
                },
            ).also { userDetailCache[intervention.userId] = it }
        } else cache
    }

    private fun createNotificationMessage(
        intervention: InterventionRecord,
        ttlSeconds: Long,
        protocol: QuestionnaireTrigger,
        userDetails: UserDetailCache,
        dataMap: Map<String, String>,
    ): Map<String, Any> {
        val notification = protocol.questionnaire.protocol.notification

        val language = userDetails.language ?: defaultLanguage
        val notificationTitle = notification.title.translation(language) ?: defaultNotificationTitle
        val notificationText = notification.text.translation(language) ?: defaultNotificationText

        val notificationBody = notificationWriter.writeValueAsString(
            AppServerNotification(
                title = notificationTitle,
                body = notificationText,
                sourceId = intervention.sourceId,
                type = protocol.questionnaire.questionnaire.name,
                ttlSeconds = ttlSeconds,
                scheduledTime = Instant.now().toString(),
                additionalData = dataMap,
            )
        )
        return appserverClient.createMessage(
            projectId = intervention.projectId,
            userId = intervention.userId,
            type = MessagingType.NOTIFICATIONS,
            contents = notificationBody,
        )
    }

    private fun createDataMessage(
        intervention: InterventionRecord,
        ttlSeconds: Long,
        dataMap: Map<String, String>,
    ): Map<String, Any> {
        val now = Instant.now().toString()
        val data = dataWriter.writeValueAsString(
            AppServerDataMessage(
                sourceId = intervention.sourceId,
                ttlSeconds = ttlSeconds,
                scheduledTime = now,
                dataMap = dataMap,
            )
        )
        return appserverClient.createMessage(
            projectId = intervention.projectId,
            userId = intervention.userId,
            type = MessagingType.DATA,
            contents = data,
        )
    }

    private fun SingleProtocolSchedule.adjustTimestamp(
        userDetails: UserDetailCache,
    ): SingleProtocolSchedule {
        val zoneId = userDetails.zoneId ?: ZoneOffset.UTC
        val now = ZonedDateTime.now(zoneId)
        val lastMidnight = now.toLocalDate().atStartOfDay(zoneId)

        val minutesSinceMidnight = Duration.between(lastMidnight, now).toMinutes()

        return copy(
            repeatQuestionnaire = when {
                repeatQuestionnaire == null -> RepeatQuestionnaire(listOf(minutesSinceMidnight))
                repeatQuestionnaire.unit == "min" -> repeatQuestionnaire.copy(
                    unitsFromZero = repeatQuestionnaire.unitsFromZero.map { it + minutesSinceMidnight }
                )
                else -> repeatQuestionnaire
            },
            referenceTimestamp = dateTimeFormatter.format(now),
        )
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AppServerIntervention::class.java)
    }
}
