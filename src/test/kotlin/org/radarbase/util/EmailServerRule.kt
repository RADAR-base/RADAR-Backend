package org.radarbase.util

import com.icegreen.greenmail.util.GreenMail
import com.icegreen.greenmail.util.ServerSetup
import jakarta.mail.MessagingException
import jakarta.mail.internet.MimeMessage
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext

class EmailServerExtension(
    val port: Int = 25251, 
    val bindAddress: String = "localhost"
) : BeforeEachCallback, AfterEachCallback {
    
    private lateinit var emailServer: GreenMail

    @Throws(MessagingException::class)
    fun messages(): Array<out MimeMessage?>? = emailServer.receivedMessages

    override fun beforeEach(context: ExtensionContext) {
        emailServer = GreenMail(
            ServerSetup(port, bindAddress, ServerSetup.PROTOCOL_SMTP),
        ).apply {
            start()
        }
    }

    override fun afterEach(context: ExtensionContext) {
        if (::emailServer.isInitialized) {
            emailServer.stop()
        }
    }
}