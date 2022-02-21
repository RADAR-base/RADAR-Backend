package org.radarcns.consumer.realtime.action.appserver

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.*

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
        val repeatQuestionnaireMinutes: Array<Long> = arrayOf(0L), // Immediately scheduled once
        val completionWindowMinutes: Long = 24 * 60L, // 1day
        val metadata: Map<String, String> = HashMap(),
        val notificationTitle: String = "Questionnaire Time",
        val notificationBody: String = "Urgent Questionnaire Pending. Please complete now.",
        val ttlSeconds: Int = 0,
        val scheduledTime: Instant,
        val sourceId: String,
        val appPackage: String = "org.phidatalab.radar_armt",
        val referenceTimestamp: Instant = scheduledTime,
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
        val metadataJson: String = try {
            ObjectMapper().writeValueAsString(metadata)
        } catch (exc: JsonProcessingException) {
            // could not process as map, use empty metadata
            logger.warn("Could not process metadata as JSON: {}", exc.message)
            "{}"
        }
        val protocolSpec = String.format(
                PROTOCOL_TEMPLATE,
                name.uppercase(),  // based on convention in protocol files
                order,
                referenceTimestamp,
                repo,
                name,
                avsc.toString(),
                repeatProtocolMinutes,
                repeatQuestionnaireMinutes.contentToString(),
                completionWindowMinutes,
                metadataJson)
        notificationMessage = String.format(
                NOTIFICATION_TEMPLATE,
                notificationTitle,
                notificationBody,
                ttlSeconds,
                sourceId,
                name,
                appPackage,
                scheduledTime,
                protocolSpec)
        dataMessage = String.format(
                DATA_TEMPLATE,
                ttlSeconds,
                sourceId,
                appPackage,
                scheduledTime,
                protocolSpec)
    }

    companion object {
        const val PROTOCOL_TEMPLATE = ("{"
                + "     \"action\" : \"QUESTIONNAIRE_TRIGGER\",\n"
                + "     \"questionnaire\": {\n"
                + "        \"name\": \"%s\",\n"
                + "        \"showIntroduction\": false,\n"
                + "        \"showInCalendar\": true,\n"
                + "        \"order\": %d,\n"
                + "        \"referenceTimestamp\": \"%s\",\n"
                + "        \"questionnaire\": {\n"
                + "          \"repository\": \"%s\",\n"
                + "          \"name\": \"%s\",\n"
                + "          \"avsc\": \"%s\"\n"
                + "        },\n"
                + "        \"startText\": {\n"
                + "          \"en\": \"\"\n"
                + "        },\n"
                + "        \"endText\": {\n"
                + "          \"en\": \"Thank you for taking the time today.\"\n"
                + "        },\n"
                + "        \"warn\": {\n"
                + "          \"en\": \"\"\n"
                + "        },\n"
                + "        \"estimatedCompletionTime\": 1,\n"
                + "        \"protocol\": {\n"
                + "          \"repeatProtocol\": {\n"
                + "            \"unit\": \"min\",\n"
                + "            \"amount\": %d\n"
                + "          },\n"
                + "          \"repeatQuestionnaire\": {\n"
                + "            \"unit\": \"min\",\n"
                + "            \"unitsFromZero\": \n"
                + "              %s\n"
                + "          },\n"
                + "          \"reminders\": {\n"
                + "            \"unit\": \"day\",\n"
                + "            \"amount\": 0,\n"
                + "            \"repeat\": 0\n"
                + "          },\n"
                + "          \"completionWindow\": {\n"
                + "            \"unit\": \"day\",\n"
                + "            \"amount\": %d\n"
                + "          }\n"
                + "        }\n"
                + "      },\n"
                + "      \"metadata\": %s\n"
                + " }"
                + "}")
        const val NOTIFICATION_TEMPLATE = ("{\n"
                + "\t\"title\" : \"%s\",\n"
                + "\t\"body\": \"%s\",\n"
                + "\t\"ttlSeconds\": %d,\n"
                + "\t\"sourceId\": \"%s\",\n"
                + "\t\"type\": \"%s\",\n"
                + "\t\"sourceType\": \"aRMT\",\n"
                + "\t\"appPackage\": \"%s\",\n"
                + "\t\"scheduledTime\": \"%s\"\n"
                + "\t\"additionalData\": \"%s\"\n"
                + " }")
        const val DATA_TEMPLATE = ("{\n"
                + "\t\"ttlSeconds\": %d,\n"
                + "\t\"sourceId\": \"%s\",\n"
                + "\t\"sourceType\": \"aRMT\",\n"
                + "\t\"appPackage\": \"%s\",\n"
                + "\t\"scheduledTime\": \"%s\"\n"
                + "\t\"dataMap\": \"%s\"\n"
                + " }")

        private val logger = LoggerFactory.getLogger(ProtocolNotificationProvider::class.java)
        private const val REPO = "https://raw.githubusercontent.com/RADAR-CNS/RADAR-REDCap-aRMT-Definitions/master/questionnaires/"
    }
}