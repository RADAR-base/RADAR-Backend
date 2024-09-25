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

package org.radarcns.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.core.Is.is;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.radarbase.config.YamlConfigLoader;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.monitor.BatteryLevelMonitor.BatteryLevelState;

public class PersistentStateStoreTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final YamlConfigLoader loader = new YamlConfigLoader();

    @Test
    public void retrieveState() throws Exception {
        File base = folder.newFolder();
        YamlPersistentStateStore stateStore = new YamlPersistentStateStore(loader, base.toPath());
        BatteryLevelState state = new BatteryLevelState();
        ObservationKey key1 = new ObservationKey("test", "a", "b");
        state.updateLevel(stateStore.keyToString(key1), 0.1f);
        stateStore.storeState("one", "two", state);

        File outputFile = new File(base, "one_two.yml");
        assertThat(outputFile.exists(), is(true));
        String rawFile = new String(Files.readAllBytes(outputFile.toPath()));
        assertThat(rawFile, equalTo("---\nlevels:\n  test#a#b: 0.1\n"));

        YamlPersistentStateStore stateStore2 = new YamlPersistentStateStore(loader, base.toPath());
        BatteryLevelState state2 = stateStore2.retrieveState("one", "two", new BatteryLevelState());
        Map<String, Float> values = state2.getLevels();
        assertThat(values, hasEntry(stateStore.keyToString(key1), 0.1f));
    }
}
