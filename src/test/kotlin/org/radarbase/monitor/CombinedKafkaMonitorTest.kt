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

package org.radarbase.monitor

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import java.io.IOException
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.stream.Stream
import kotlin.test.assertFailsWith
import kotlin.test.junit5.JUnit5Asserter.fail

class CombinedKafkaMonitorTest {

    @Test
    fun testExceptionFlow() {
        assertFailsWith(IOException::class) {
            val kafkaMonitor1 = mock(KafkaMonitor::class.java)
            val kafkaMonitor2 = mock(KafkaMonitor::class.java)

            `when`(kafkaMonitor2.start()).thenThrow(IOException("failed to run!"))

            val km = CombinedKafkaMonitor(Stream.of(kafkaMonitor1, kafkaMonitor2))

            try {
                km.start()
            } catch (ex: IOException) {
                verify(kafkaMonitor1, times(1)).start()
                verify(kafkaMonitor2, times(1)).start()
                verify(kafkaMonitor1, times(1)).shutdown()
                verify(kafkaMonitor2, times(1)).shutdown()
                assertTrue(km.isShutdown)
                throw ex
            }
        }
    }

    @Test
    fun testFlow() {
        val kafkaMonitor1 = mock(KafkaMonitor::class.java)
        val kafkaMonitor2 = mock(KafkaMonitor::class.java)

        val km = CombinedKafkaMonitor(Stream.of(kafkaMonitor1, kafkaMonitor2))

        val executor = Executors.newSingleThreadExecutor()
        executor.submit {
            try {
                km.start()
            } catch (e: Exception) {
                fail(e.toString())
            }
        }

        Thread.sleep(100L)

        assertFalse(km.isShutdown)
        verify(kafkaMonitor1, times(1)).start()
        verify(kafkaMonitor2, times(1)).start()

        km.shutdown()
        verify(kafkaMonitor1, times(1)).shutdown()
        verify(kafkaMonitor2, times(1)).shutdown()

        assertTrue(km.isShutdown)
        executor.shutdown()
        assertTrue(executor.awaitTermination(100, TimeUnit.MILLISECONDS))
    }

    @Test
    fun testPollTimeout() {
        val kafkaMonitor1 = mock(KafkaMonitor::class.java)
        val kafkaMonitor2 = mock(KafkaMonitor::class.java)

        val km = CombinedKafkaMonitor(Stream.of(kafkaMonitor1, kafkaMonitor2))
        km.pollTimeout = Duration.ofSeconds(1L)

        verify(kafkaMonitor1, times(1)).pollTimeout = Duration.ofSeconds(1L)
        verify(kafkaMonitor2, times(1)).pollTimeout = Duration.ofSeconds(1L)
    }

    @Test
    fun testEmpty() {
        assertFailsWith(IllegalArgumentException::class) {
            CombinedKafkaMonitor(Stream.empty())
        }
    }

    @Test
    fun testNull() {
        assertFailsWith(NullPointerException::class) {
            CombinedKafkaMonitor(null!!)
        }
    }
}
