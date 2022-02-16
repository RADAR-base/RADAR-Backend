package org.radarcns.monitor.intervention

import org.radarcns.util.EmailSenders
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.ZoneOffset

class InterventionExceptionEmailer(
    private val emailSenders: EmailSenders,
    private val state: InterventionMonitorState,
) {
    fun emailExceptions() {
        state.exceptions.forEach { (projectId, projectExceptions) ->
            val userMessages: List<String> = projectExceptions.exceptions
                .entries
                .map { (userId, userExceptions) ->
                    val exceptionList = userExceptions.lines
                    val numLines = exceptionList.size
                    if (userExceptions.isTruncated) {
                        exceptionList.addFirst("...")
                    }
                    val exceptionString = exceptionList.joinToString(separator = "") { "$exceptionPrefix$it" }
                    "user $userId - listing $numLines out of ${userExceptions.count} exceptions:$exceptionString"
                }

            val totalCount = projectExceptions.exceptions.values.sumOf { it.count }

            val date = LocalDate.ofInstant(state.fromDate, ZoneOffset.UTC).toString()

            val subject = "[RADAR-base $projectId] Errors in intervention algorithm on $date"
            val message = """
                Hi,
                
                On $date, some errors occurred in the RADAR-base intervention algorithm. This message summarizes the errors occurred for the RADAR-base $projectId project in the last 24 hours. A total of $totalCount errors were counted. Below is a summary of the errors:
                
                ${userMessages.joinToString(separator = "\n\n")}
                
                This is an automated message from the RADAR-base platform. Please refer to your RADAR-base administrator for more information.
                """.trimIndent()

            val sender = emailSenders.getEmailSenderForProject(projectId)

            if (sender != null) {
                sender.sendEmail(subject, message)
            } else {
                logger.warn("No email sender configured for project {}. Not sending exception message.", projectId)
            }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(InterventionExceptionEmailer::class.java)

        private const val exceptionPrefix = "\n - "
    }
}
