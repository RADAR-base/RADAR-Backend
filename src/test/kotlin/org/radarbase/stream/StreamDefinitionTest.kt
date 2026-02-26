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

package org.radarbase.stream

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.radarbase.stream.AbstractStreamWorker.Companion.OUTPUT_LABEL
import org.radarbase.topic.KafkaTopic
import java.util.regex.Pattern
import kotlin.test.assertFailsWith

class StreamDefinitionTest {
    companion object {
        private val TOPIC_PATTERN = Pattern.compile("^[A-Za-z0-9_-]+$")
        private const val INPUT = "android_empatica_e4_blood_volume_pulse"
        private const val OUTPUT = INPUT + OUTPUT_LABEL
    }

    @Test
    fun nameValidation() {
        val inputTopic = KafkaTopic(INPUT)
        val outputTopic = KafkaTopic(OUTPUT)

        val definition = StreamDefinition(inputTopic, outputTopic)

        assertTrue(TOPIC_PATTERN.matcher(definition.stateStoreName).matches())
        assertEquals(
            "From-android_empatica_e4_blood_volume_pulse-To-android_empatica_e4_blood_volume_pulse_output",
            definition.stateStoreName,
        )
    }

    @Test
    fun faultyNameValidation() {
        assertFailsWith(IllegalArgumentException::class) {
            val inputTopic = KafkaTopic("$INPUT$")
            val outputTopic = KafkaTopic(OUTPUT)

            val definition = StreamDefinition(inputTopic, outputTopic)
            assertFalse(TOPIC_PATTERN.matcher(definition.stateStoreName).matches())
        }
    }
}
