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
import org.radarcns.key.MeasurementKey;
import org.radarcns.monitor.BatteryLevelMonitor.BatteryLevelState;

public class PersistentStateStoreTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void retrieveState() throws Exception {
        File base = folder.newFolder();
        PersistentStateStore stateStore = new PersistentStateStore(base);
        BatteryLevelState state = new BatteryLevelState();
        MeasurementKey key1 = new MeasurementKey("a", "b");
        state.updateLevel(key1, 0.1f);
        stateStore.storeState("one", "two", state);
        File outputFile = new File(base, "one_two.yml");
        assertThat(outputFile.exists(), is(true));

        String rawFile = new String(Files.readAllBytes(outputFile.toPath()));
        assertThat(rawFile, equalTo("---\nlevels:\n  a#b: 0.1\n"));

        PersistentStateStore stateStore2 = new PersistentStateStore(base);
        BatteryLevelState state2 = stateStore2.retrieveState("one", "two", new BatteryLevelState());
        Map<String, Float> values = state2.getLevels();
        assertThat(values, hasEntry("a#b", 0.1f));
    }
}