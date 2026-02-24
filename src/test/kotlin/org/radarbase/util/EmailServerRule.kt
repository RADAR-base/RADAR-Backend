package org.radarbase.util

import com.icegreen.greenmail.util.GreenMail
import com.icegreen.greenmail.util.ServerSetup
import com.icegreen.greenmail.util.ServerSetupTest
import org.junit.rules.ExternalResource
import jakarta.mail.MessagingException
import jakarta.mail.internet.MimeMessage

class EmailServerRule(val port: Int = 25251, val bindAddress: String = "localhost") : ExternalResource() {
    private lateinit var emailServer: GreenMail

    @Throws(MessagingException::class)
    fun messages(): Array<out MimeMessage?>? = emailServer.receivedMessages

    override fun before() {
        emailServer = GreenMail(
            ServerSetup(port, bindAddress, ServerSetup.PROTOCOL_SMTP)
        ).apply { start() }
    }

    override fun after() {
        if (::emailServer.isInitialized) {
            emailServer.stop()
        }
    }
}
