package org.radarcns.consumer.realtime.action.appserver

import org.radarbase.appserver.client.protocol.*
import org.radarcns.consumer.realtime.Grouping.Companion.objectMapper
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * The content provides a questionnaire's protocol block in the message to be added to the appserver
 * and for the aRMT app to parse and schedule the specified questionnaire. Supports both FCM
 * Notification and Data Messages.
 */

class ProtocolNotificationProvider(
        val repo: String = REPO,
        val order: Int = 0,
        val name: String,
        val avsc: AvscTypes = AvscTypes.QUESTIONNAIRE,
        val repeatProtocolMinutes: Long = 9999999999L, // a lot of years, it will not repeat
        val completionWindowMinutes: Long = 24 * 60L, // 1day
        val metadata: Map<String, String?> = HashMap(),
        val notificationTitle: String = "Questionnaire Time",
        val notificationBody: String = "Urgent Questionnaire Pending. Please complete now.",
        val ttlSeconds: Int = 0,
        val scheduledTime: Instant,
        val sourceId: String? = null,
        val appPackage: String = "org.phidatalab.radar_armt",
        val referenceTimestamp: Instant = scheduledTime,
        val userTimezone: String = "UTC",
        val repeatQuestionnaireMinutes: Array<Long> =
                arrayOf(referenceTimestamp.timeSinceMidnight(userTimezone)),
) : NotificationContentProvider {

    override val notificationMessage: String
    override val dataMessage: String

    enum class AvscTypes(val type: String) {
        QUESTIONNAIRE("questionnaire"),
        NOTIFICATION("notification");

        override fun toString(): String {
            return type
        }
    }

    init {

        val questionnaire = SingleProtocol(
                name = name,
                order = order,
                questionnaire = Questionnaire(
                        name = name,
                        avsc = avsc.toString(),
                        repository = repo,
                ),
                protocol = SingleProtocolSchedule(
                        completionWindow = ProtocolDuration(
                                amount = completionWindowMinutes,
                                unit = "min",

                                ),
                        repeatProtocol = ProtocolDuration(
                                amount = repeatProtocolMinutes,
                                unit = "min",

                                ),
                        repeatQuestionnaire = RepeatQuestionnaire(
                                unitsFromZero = repeatQuestionnaireMinutes.map { it.toInt() }.toList(),
                                unit = "min"
                        ),
                        referenceTimestamp = referenceTimestamp
                                .atZone(ZoneId.of(userTimezone))
                                .format(DateTimeFormatter.ofPattern("dd-MM-yyyy:HH:mm")),
                        //.format(DateTimeFormatter.ISO_ZONED_DATE_TIME),
                ),
        )

        val trigger = QuestionnaireTrigger(
                singleProtocol = questionnaire,
                metadataMap = metadata,
        )

        notificationMessage = String.format(
                NOTIFICATION_TEMPLATE,
                notificationTitle,
                notificationBody,
                ttlSeconds,
                sourceId,
                name,
                appPackage,
                scheduledTime,
                objectMapper.writeValueAsString(trigger))
        dataMessage = String.format(
                DATA_TEMPLATE,
                ttlSeconds,
                sourceId,
                appPackage,
                scheduledTime,
                objectMapper.writeValueAsString(trigger))

        logger.debug("Notification message: {}", notificationMessage)
        logger.debug("Data message: {}", notificationMessage)
    }

    companion object {
        const val NOTIFICATION_TEMPLATE = ("{\n"
                + "\t\"title\" : \"%s\",\n"
                + "\t\"body\": \"%s\",\n"
                + "\t\"ttlSeconds\": %d,\n"
                + "\t\"sourceId\": \"%s\",\n"
                + "\t\"type\": \"%s\",\n"
                + "\t\"sourceType\": \"aRMT\",\n"
                + "\t\"appPackage\": \"%s\",\n"
                + "\t\"scheduledTime\": \"%s\",\n"
                + "\t\"additionalData\": %s\n"
                + " }")

        const val DATA_TEMPLATE = ("{\n"
                + "\t\"ttlSeconds\": %d,\n"
                + "\t\"sourceId\": \"%s\",\n"
                + "\t\"sourceType\": \"aRMT\",\n"
                + "\t\"appPackage\": \"%s\",\n"
                + "\t\"scheduledTime\": \"%s\",\n"
                + "\t\"priority\": \"HIGH\",\n"
                + "\t\"dataMap\": %s\n"
                + " }")

        private val logger = LoggerFactory.getLogger(ProtocolNotificationProvider::class.java)
        private const val REPO = "https://raw.githubusercontent.com/RADAR-base/RADAR-REDCap-aRMT-Definitions/master/questionnaires"

        fun Instant.timeSinceMidnight(timezone: String = "UTC"): Long {
            val zoned = atZone(ZoneId.of(timezone))
            val midnight = zoned.toLocalDate().atStartOfDay(zoned.zone).toInstant()
            val duration = Duration.between(midnight, this)
            return duration.toMinutes()
        }
    }
}