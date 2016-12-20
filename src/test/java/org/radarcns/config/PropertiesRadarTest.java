package org.radarcns.config;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.yaml.snakeyaml.error.YAMLException;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by nivethika on 19-12-16.
 */
public class PropertiesRadarTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void getInstanceEmptyProperties() throws NoSuchFieldException, IllegalAccessException, SecurityException {
        Field instance = PropertiesRadar.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Property cannot be accessed without calling load() first");
        PropertiesRadar.getInstance();
    }

    @Test
    public void loadWithEmptyPath() throws Exception {
        Field instance = PropertiesRadar.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Config file does not exist");
        PropertiesRadar.load(null);
    }

    @Test
    public void loadWithInvalidFilePath() throws Exception {
        Field instance = PropertiesRadar.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Config file is invalid");
        String invalidPath = "/usr/";
        PropertiesRadar.load(invalidPath);
    }

    @Test
    public void load() throws Exception {
        Field instance = PropertiesRadar.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);
        PropertiesRadar.load("radar.yml");

        ConfigRadar properties = PropertiesRadar.getInstance();
        assertEquals("standalone", properties.getMode());
        assertNull(properties.getLog_path());
        assertNotNull(properties.getBroker());
        assertNotNull(properties.getBrokerPath());
        assertNotNull(properties.getReleased());
        assertNotNull(properties.getSchema_registry());
        assertNotNull(properties.getSchemaRegistryPath());
        assertNotNull(properties.getZookeeper());
        assertNotNull(properties.getAuto_commit_interval_ms());
        assertNotNull(properties.getSession_timeout_ms());
        assertNotNull(properties.getZookeeperPath());
        assertNotNull(properties.getVersion());

    }

    @Test
    public void loadInvalidYaml() throws Exception {
        exception.expect(YAMLException.class);
        PropertiesRadar.load("src/test/resources/config/invalidradar.yml");
    }

    @Test
    public void loadWithInstance() throws Exception {
        Field instance = PropertiesRadar.class.getDeclaredField("instance");
        instance.setAccessible(true);
        instance.set(null, null);

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Property class has been already loaded");
        PropertiesRadar.load("radar.yml");
        PropertiesRadar.load("again.yml");
    }



}
