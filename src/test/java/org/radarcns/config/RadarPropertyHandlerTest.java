package org.radarcns.config;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.yaml.snakeyaml.error.YAMLException;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * Created by nivethika on 19-12-16.
 */
public class RadarPropertyHandlerTest {

    private RadarPropertyHandler propertyHandler ;

    @Before
    public void setUp() {
        this.propertyHandler = new RadarPropertyHandlerImpl();
    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void getInstanceEmptyProperties() throws NoSuchFieldException, IllegalAccessException, SecurityException {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Properties cannot be accessed without calling load() first");
        propertyHandler.getRadarProperties();
    }

    @Test
    public void loadWithEmptyPath() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Config file does not exist");
        propertyHandler.load(null);
    }

    @Test
    public void loadWithInvalidFilePath() throws Exception {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Config file is invalid");
        String invalidPath = "/usr/";
        propertyHandler.load(invalidPath);
    }

    @Test
    public void load() throws Exception {
        propertyHandler.load("radar.yml");

        ConfigRadar properties = propertyHandler.getRadarProperties();
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
        propertyHandler.load("src/test/resources/config/invalidradar.yml");
    }

    @Test
    public void loadWithInstance() throws Exception {

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Properties class has been already loaded");
        propertyHandler.load("radar.yml");
        propertyHandler.load("again.yml");
        ConfigRadar properties = propertyHandler.getRadarProperties();
        assertNotNull(properties);
    }

    @Test
    public void loadWithLogPath() throws Exception{
        RadarPropertyHandlerImpl radarPropertyHandler = mock(RadarPropertyHandlerImpl.class);
        ConfigRadar configs = mock(ConfigRadar.class);
        when(radarPropertyHandler.getRadarProperties()).thenReturn(configs);
        when(configs.getLog_path()).thenReturn("src/test");

        doCallRealMethod().when(radarPropertyHandler).load("radar.yml");
        radarPropertyHandler.load("radar.yml");
        verify(configs, times(2)).getLog_path();
    }

    @Test
    public void loadWithInvalidLogPath() throws Exception{

        RadarPropertyHandlerImpl radarPropertyHandler = mock(RadarPropertyHandlerImpl.class);
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("User Log path does not exist");
        ConfigRadar configs = mock(ConfigRadar.class);
        when(radarPropertyHandler.getRadarProperties()).thenReturn(configs);
        when(configs.getLog_path()).thenReturn("hack.log");
        doCallRealMethod().when(radarPropertyHandler).load("radar.yml");
        radarPropertyHandler.load("radar.yml");
        verify(configs, times(2)).getLog_path();

    }

    @Test
    public void loadWithFileLogPath() throws Exception{

        RadarPropertyHandlerImpl radarPropertyHandler = mock(RadarPropertyHandlerImpl.class);
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("User Log path is not a directory");
        ConfigRadar configs = mock(ConfigRadar.class);
        when(radarPropertyHandler.getRadarProperties()).thenReturn(configs);
        when(configs.getLog_path()).thenReturn("backend.log");
        doCallRealMethod().when(radarPropertyHandler).load("radar.yml");
        radarPropertyHandler.load("radar.yml");
        verify(configs, times(2)).getLog_path();

    }

    @Test
    public void getKafkaPropertiesBeforeLoad()
    {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Properties cannot be accessed without calling load() first");
        KafkaProperty property =propertyHandler.getKafkaProperties();
        assertNull(property);
    }

    @Test
    public void getKafkaProperties() throws Exception
    {
        propertyHandler.load("radar.yml");
        KafkaProperty property =propertyHandler.getKafkaProperties();
        assertNotNull(property);
    }

}
