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

package org.radarcns.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.radarcns.stream.AbstractStreamWorker.OUTPUT_LABEL;

import java.util.regex.Pattern;
import org.junit.Test;
import org.radarbase.topic.KafkaTopic;

public class StreamDefinitionTest {
    private static final Pattern TOPIC_PATTERN = Pattern.compile("^[A-Za-z0-9_-]+$");

    private static final String INPUT = "android_empatica_e4_blood_volume_pulse";
    private static final String OUTPUT = INPUT + OUTPUT_LABEL;

    @Test
    public void nameValidation() {
        KafkaTopic inputTopic = new KafkaTopic(INPUT);
        KafkaTopic outputTopic = new KafkaTopic(OUTPUT);

        StreamDefinition definition = new StreamDefinition(inputTopic, outputTopic);

        assertTrue(TOPIC_PATTERN.matcher(definition.getStateStoreName()).matches());
        assertEquals("From-android_empatica_e4_blood_volume_pulse"
                        + "-To-android_empatica_e4_blood_volume_pulse_output",
                definition.getStateStoreName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void faultyNameValidation() {
        KafkaTopic inputTopic = new KafkaTopic(INPUT + "$");
        KafkaTopic outputTopic = new KafkaTopic(OUTPUT);

        StreamDefinition definition = new StreamDefinition(inputTopic, outputTopic);
        assertFalse(TOPIC_PATTERN.matcher(definition.getStateStoreName()).matches());
    }
}
