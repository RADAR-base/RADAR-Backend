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

import java.io.IOException
import java.util.*
import jakarta.mail.Message
import jakarta.mail.MessagingException
import jakarta.mail.Session
import jakarta.mail.Transport
import jakarta.mail.internet.InternetAddress
import jakarta.mail.internet.MimeMessage

/**
 * Sends emails.
 */
class EmailSender @Throws(IOException::class) constructor(
    host: String?,
    port: Int,
    private val from: String?,
    private val to: List<String>
) {
    private val session: Session

    init {
        val properties = Properties()
        // Get system properties
        properties.putAll(System.getProperties())

        if (host != null) {
            // Setup mail server
            properties.setProperty("mail.smtp.host", host)
        }
        if (port > 0) {
            properties.setProperty("mail.smtp.port", port.toString())
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
        // Create a default MimeMessage object.
        val message = MimeMessage(session)

        // Set From: header field of the header.
        message.setFrom(InternetAddress(from))

        for (recipient in to) {
            // Set To: header field of the header.
            message.addRecipient(Message.RecipientType.TO, InternetAddress(recipient))
        }

        // Set Subject: header field
        message.subject = subject

        // Now set the actual message
        message.setText(text)

        // Send message
        Transport.send(message)
    }
}
