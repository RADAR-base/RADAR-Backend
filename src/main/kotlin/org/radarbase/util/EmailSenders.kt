package org.radarbase.util

import org.radarbase.config.MonitorConfig
import java.io.IOException

/**
 * Class to store [EmailSender] associated with each project.
 */
class EmailSenders(private val emailSenderMap: Map<String, EmailSender>) {

    /**
     * Parses the [MonitorConfig] to map the corresponding
     * [EmailSender] to each project. A project can have a list of
     * associated email addresses.
     *
     * @param config  Configuration of the Monitor containing project
     *                 and email address mapping
     * @throws IOException
     */
    companion object {
        @JvmStatic
        @Throws(IOException::class)
        fun parseConfig(config: MonitorConfig): EmailSenders {
            val map = config.notifyConfig!!.associate { notifyConfig ->
                notifyConfig.projectId to EmailSender(
                    config.emailHost,
                    config.emailPort,
                    config.emailUser,
                    notifyConfig.emailAddress,
                )
            }
            return EmailSenders(map)
        }
    }

    fun getEmailSenderForProject(projectId: String): EmailSender? {
        return emailSenderMap[projectId]
    }
}
