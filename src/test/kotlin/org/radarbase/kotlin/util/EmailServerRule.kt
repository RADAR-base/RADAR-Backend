package org.radarbase.kotlin.util

import org.junit.rules.ExternalResource
import org.subethamail.wiser.Wiser
import javax.mail.MessagingException
import javax.mail.internet.MimeMessage

class EmailServerRule(val port: Int = 25251) : ExternalResource() {
    private lateinit var emailServer: Wiser

    @Throws(MessagingException::class)
    fun messages() = emailServer.messages.map { it.mimeMessage }

    override fun before() {
        emailServer = Wiser(port).apply {
            setHostname("localhost")
            start()
        }
    }

    override fun after() {
        if (::emailServer.isInitialized) {
            emailServer.stop()
        }
    }
}
