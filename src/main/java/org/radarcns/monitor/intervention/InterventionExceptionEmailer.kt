package org.radarcns.monitor.intervention

import org.radarcns.util.EmailSenders
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset

class InterventionExceptionEmailer(
    private val emailSenders: EmailSenders,
    private val state: InterventionMonitorState,
) {
    fun emailExceptions(uptoTime: Instant) {
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
                        "* $userId - listing $numLines out of ${userExceptions.count} exceptions:$exceptionString"
                    }

                val totalCount = projectExceptions.exceptions.values.sumOf { it.count }

                val date = LocalDate.ofInstant(state.fromDate, ZoneOffset.UTC).toString()

                val subject = "[RADAR-base $projectId] Errors in intervention algorithm on $date"
                val message = """
                    |Hi,
                    |
                    |On $date, some errors occurred in the RADAR-base intervention algorithm. This message summarizes errors in the RADAR-base $projectId project from ${state.fromDate} to ${uptoTime}. A total of $totalCount errors were counted. Below is a summary of the errors per user:
                    |
                    |${userMessages.joinToString(separator = "\n\n")}
                    |
                    |This is an automated message from the RADAR-base platform. Please refer to your RADAR-base administrator for more information.
                """.trimMargin()

                logger.info("Sending message for project {}:\n=====\n{}\n=====", projectId, message)

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

        private const val exceptionPrefix = "\n   - "
    }
}
