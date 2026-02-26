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

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.radarbase.monitor.BatteryLevelMonitor.BatteryLevelState
import org.radarcns.kafka.ObservationKey
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals

class PersistentStateStoreTest {

    @TempDir
    lateinit var folder: Path

    @Test
    fun retrieveState() {
        val base = folder.toFile()
        val stateStore = YamlPersistentStateStore(base)
        val state = BatteryLevelState()
        val key1 = ObservationKey("test", "a", "b")
        state.updateLevel(stateStore.keyToString(key1), 0.1f)
        stateStore.storeState("one", "two", state)

        val outputFile = File(base, "one_two.yml")
        assert(outputFile.exists())
        val rawFile = String(Files.readAllBytes(outputFile.toPath()))
        assertEquals(rawFile, "---\nlevels:\n  test#a#b: 0.1\n")

        val stateStore2 = YamlPersistentStateStore(base)
        val state2 = stateStore2.retrieveState("one", "two", BatteryLevelState())
        val values: Map<String, Float> = state2.levels
        assert(values.containsKey(stateStore.keyToString(key1)))
        assertEquals(0.1f, values.get(stateStore.keyToString(key1)))
    }
}
