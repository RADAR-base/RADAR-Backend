package org.radarcns.monitor.intervention

import org.radarcns.util.EmailSenders
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.ZonedDateTime

class InterventionExceptionEmailer(
    private val emailSenders: EmailSenders,
    private val state: InterventionMonitorState,
) {
    fun emailExceptions() {
        state.exceptions.forEach { (projectId, projectExceptions) ->
            try {
                val userMessages: List<String> = projectExceptions.exceptions
                    .entries
                    .map { (userId, userExceptions) ->
                        val exceptionList = userExceptions.lines
                        val numLines = exceptionList.size
                        if (userExceptions.isTruncated) {
                            exceptionList.addFirst("...")
                        }
                        val exceptionString =
                            exceptionList.joinToString(separator = "") { "$exceptionPrefix$it" }
                        "user $userId - listing $numLines out of ${userExceptions.count} exceptions:$exceptionString"
                    }

                val totalCount = projectExceptions.exceptions.values.sumOf { it.count }

                val date = LocalDate.ofInstant(state.fromDate, ZoneOffset.UTC).toString()
                val start = ZonedDateTime.ofInstant(state.fromDate, ZoneOffset.UTC)
                val end = ZonedDateTime.ofInstant(state.nextMidnight(), ZoneOffset.UTC)

                val subject = "[RADAR-base $projectId] Errors in intervention algorithm on $date"
                val message = """
                Hi,
                
                On $date, some errors occurred in the RADAR-base intervention algorithm. This message summarizes the errors occurred for the RADAR-base $projectId project from $start to $end. A total of $totalCount errors were counted. Below is a summary of the errors:
                
                ${userMessages.joinToString(separator = "\n\n")}
                
                This is an automated message from the RADAR-base platform. Please refer to your RADAR-base administrator for more information.
                """.trimIndent()

                logger.info("Sending message for project {}: {}", projectId, message)

                val sender = emailSenders.getEmailSenderForProject(projectId)

                if (sender != null) {
                    sender.sendEmail(subject, message)
                } else {
                    logger.warn(
                        "No email sender configured for project {}. Not sending exception message.",
                        projectId
                    )
                }
            } catch (ex: Throwable) {
                logger.error("Failed to send error notifications for project {} - {}: {}",
                    projectId, projectExceptions, ex.toString())
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(InterventionExceptionEmailer::class.java)

        private const val exceptionPrefix = "\n - "
    }
}
