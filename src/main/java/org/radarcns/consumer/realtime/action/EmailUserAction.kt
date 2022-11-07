package org.radarcns.consumer.realtime.action

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.radarcns.config.EmailServerConfig
import org.radarcns.config.realtime.ActionConfig
import org.radarcns.consumer.realtime.action.EmailUserAction
import org.radarcns.util.EmailSender
import org.slf4j.LoggerFactory
import java.time.Instant
import javax.mail.MessagingException

/**
 * This action can be used to trigger an email to the user. Currently, it just notifies that the
 * conditions evaluated to true and provides some context. This is useful for project admins but can
 * be modified to also work as an intervention mechanism in some use-cases.
 */
class EmailUserAction(
        actionConfig: ActionConfig,
        emailServerConfig: EmailServerConfig?,
        override val name: String = NAME,
) : ActionBase(actionConfig) {

    private val props = actionConfig.properties

    @Suppress("UNCHECKED_CAST")
    private val emailSender: EmailSender = EmailSender(
            emailServerConfig,
            (props?.get("from") as String?)
                    ?: throw IllegalArgumentException("Missing 'from' property"),
            props?.getOrDefault("email_addresses", ArrayList<String>()) as List<String?>)

    private val customTitle: String? = props?.getOrDefault("title", null) as String?
    private val customBody: String? = props?.getOrDefault("body", null) as String?

    override fun executeFor(record: ConsumerRecord<*, *>?): Boolean {
        val key = getKeys(record)

        val title: String = if (customTitle.isNullOrEmpty()) {
            "Conditions triggered the action $name for user" +
                    " ${key?.userId}) from topic ${record?.topic()}"
        } else customTitle

        val body: String = if (customBody.isNullOrEmpty()) {
            """
             Record: 
             ${record?.value()?.toString()}
             
             Timestamp: ${Instant.now()}
             Key: ${key.toString()}
            """.trimIndent()
        } else customBody

        return try {
            emailSender.sendEmail(title, body)
            logger.info("Email sent to admin for project ${key?.projectId}, user ${key?.userId}")
            true
        } catch (e: MessagingException) {
            logger.error("Error sending email", e)
            false
        }
    }

    companion object {
        const val NAME = "EmailUserAction"
        private val logger = LoggerFactory.getLogger(EmailUserAction::class.java)
    }
}