package org.radarcns.monitor.intervention

import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.OkHttpClient
import org.radarbase.appserver.client.AppServerData
import org.radarbase.appserver.client.AppServerNotification
import org.radarbase.appserver.client.AppserverClient
import org.radarbase.appserver.client.MessagingType
import org.radarbase.appserver.client.protocol.Notification.Companion.defaultNotificationText
import org.radarbase.appserver.client.protocol.Notification.Companion.defaultNotificationTitle
import org.radarbase.appserver.client.protocol.ProtocolDirectory
import org.radarbase.appserver.client.protocol.QuestionnaireTrigger
import org.radarbase.appserver.client.protocol.translation
import org.radarcns.config.monitor.AuthConfig
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

class AppServerIntervention(
        private val protocolDirectory: ProtocolDirectory,
        appserverUrl: String,
        private val defaultLanguage: String,
        authConfig: AuthConfig,
        httpClient: OkHttpClient,
        mapper: ObjectMapper,
) {
    private val appserverClient: AppserverClient
    private val protocolWriter = mapper.writerFor(QuestionnaireTrigger::class.java)
    private val notificationWriter = mapper.writerFor(AppServerNotification::class.java)
    private val dataWriter = mapper.writerFor(AppServerData::class.java)

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

        val body = protocolWriter.writeValueAsString(protocol)

        val ttlSeconds = ttl.toSeconds()
        val notificationResponse = createNotificationMessage(intervention, ttlSeconds, protocol, body)
        val dataResponse = createDataMessage(intervention, ttlSeconds, body)

        logger.debug("Created App Server message for notification {} and data {}",
                notificationResponse, dataResponse)
    }

    private fun createNotificationMessage(
            intervention: InterventionRecord,
            ttlSeconds: Long,
            protocol: QuestionnaireTrigger,
            body: String,
    ): Map<String, Any> {
        val notification = protocol.singleProtocol.protocol.notification

        val language = try {
            (appserverClient
                    .getUserDetails(intervention.projectId, intervention.userId)["language"]
                    ?: defaultLanguage) as String

        } catch (e: Exception) {
            logger.warn("Failed to get user language for ${intervention.userId}. Using default", e)
            defaultLanguage
        }

        val notificationTitle = notification.title.translation(language) ?: defaultNotificationTitle
        val notificationText = notification.text.translation(language) ?: defaultNotificationText

        val notificationBody = notificationWriter.writeValueAsString(
                AppServerNotification(
                        title = notificationTitle,
                        body = notificationText,
                        sourceId = intervention.sourceId,
                        type = protocol.singleProtocol.questionnaire.name,
                        ttlSeconds = ttlSeconds,
                        scheduledTime = Instant.now().toString(),
                        additionalData = body,
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
            body: String,
    ): Map<String, Any> {
        val now = Instant.now().toString()
        val data = dataWriter.writeValueAsString(
                AppServerData(
                        sourceId = intervention.sourceId,
                        ttlSeconds = ttlSeconds,
                        scheduledTime = now,
                        dataMap = body,
                )
        )
        return appserverClient.createMessage(
                projectId = intervention.projectId,
                userId = intervention.userId,
                type = MessagingType.DATA,
                contents = data,
        )
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AppServerIntervention::class.java)
    }
}
