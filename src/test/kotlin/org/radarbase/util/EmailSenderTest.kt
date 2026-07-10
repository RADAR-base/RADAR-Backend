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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.io.IOException
import kotlin.test.assertFailsWith

class EmailSenderTest {

    @RegisterExtension
    @JvmField
    val emailServer = EmailServerExtension(port = 2525)

    @Test
    fun testEmail() {
        val sender = EmailSender("localhost", 2525, "no-reply@radar-cns.org", listOf("test@radar-cns.org"))

        assertEquals(0, emailServer.messages()?.size)

        sender.sendEmail("hi", "it's me")

        val messages = emailServer.messages()
        assertNotNull(messages)
        assertEquals(1, messages!!.size)
        val mime = messages[0]
        assertNotNull(mime)

        assertEquals(1, mime!!.from.size)
        assertEquals("no-reply@radar-cns.org", mime.from[0].toString())

        val to = mime.getRecipients(Message.RecipientType.TO)
        assertEquals(1, to.size)
        assertEquals("test@radar-cns.org", to[0].toString())

        assertEquals("hi", mime.subject)
        assertEquals("it's me", mime.content.toString().trim())
    }

    @Test
    fun testEmailNonExisting() {
        assertFailsWith(IOException::class) {
            EmailSender("non-existing-host", 2525, "no-reply@radar-cns.org", listOf("test@radar-cns.org"))
        }
    }
}
