/*
 * Copyright 2017 King's College London and The Hyve
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarbase.util

import jakarta.mail.Message
import jakarta.mail.MessagingException
import jakarta.mail.Session
import jakarta.mail.Transport
import jakarta.mail.internet.InternetAddress
import jakarta.mail.internet.MimeMessage
import java.io.IOException
import java.util.*

/**
 * Sends emails.
 */
class EmailSender
@Throws(IOException::class)
constructor(
    host: String,
    port: Int,
    private val from: String?,
    private val to: List<String>,
) {
    private val session: Session

    init {
        assert(port > 0) { "Port must be positive" }

        val properties = Properties().apply {
            this.putAll(System.getProperties())
            this.setProperty("mail.smtp.host", host)
            this.setProperty("mail.smtp.port", port.toString())
        }

        session = Session.getInstance(properties)
        try {
            session.getTransport("smtp").use { transport ->
                transport.connect()
                if (!transport.isConnected) {
                    throw IOException("Cannot connect to SMTP server $host:$port")
                }
            }
        } catch (ex: MessagingException) {
            throw IOException("Cannot instantiate SMTP server", ex)
        }
    }

    /**
     * Send an email with given subject and text. The pre-configured From and To headers are used.
     * @param subject email subject
     * @param text plain text content of the email
     * @throws MessagingException if the message could not be sent
     */
    @Throws(MessagingException::class)
    fun sendEmail(subject: String, text: String) {
        val from = from ?: throw IllegalArgumentException("No From address specified")
        val message = MimeMessage(session).apply {
            this.setFrom(InternetAddress(from))
            for (recipient in to) {
                this.addRecipient(Message.RecipientType.TO, InternetAddress(recipient))
            }
            this.subject = subject
            this.setText(text)
        }
        Transport.send(message)
    }
}
