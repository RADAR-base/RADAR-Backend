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

package org.radarbase.kotlin.util

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.equalTo
import org.hamcrest.Matchers.hasEntry
import org.hamcrest.core.Is.`is`
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.radarbase.kotlin.monitor.BatteryLevelMonitor.BatteryLevelState
import org.radarcns.kafka.ObservationKey
import java.io.File
import java.nio.file.Files

class PersistentStateStoreTest {
    @Rule @JvmField
    val folder = TemporaryFolder()

    @Test
    fun retrieveState() {
        val base: File = folder.newFolder()
        val stateStore = YamlPersistentStateStore(base)
        val state = BatteryLevelState()
        val key1 = ObservationKey("test", "a", "b")
        state.updateLevel(stateStore.keyToString(key1), 0.1f)
        stateStore.storeState("one", "two", state)

        val outputFile = File(base, "one_two.yml")
        assertThat(outputFile.exists(), `is`(true))
        val rawFile = String(Files.readAllBytes(outputFile.toPath()))
        assertThat(rawFile, equalTo("---\nlevels:\n  test#a#b: 0.1\n"))

        val stateStore2 = YamlPersistentStateStore(base)
        val state2 = stateStore2.retrieveState("one", "two", BatteryLevelState())
        val values: Map<String, Float> = state2.levels
        assertThat(values, hasEntry(stateStore.keyToString(key1), 0.1f))
    }
}
