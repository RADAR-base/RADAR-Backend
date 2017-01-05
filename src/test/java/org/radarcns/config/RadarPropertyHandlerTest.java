package org.radarcns.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import java.lang.reflect.Field;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by nivethika on 19-12-16.
 */
public class RadarPropertyHandlerTest {

    private RadarPropertyHandler propertyHandler ;

    @Before
    public void setUp() throws NoSuchFieldException, IllegalAccessException {

        this.propertyHandler = new RadarPropertyHandlerImpl();

    }

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void getInstanceEmptyProperties() throws NoSuchFieldException, IllegalAccessException, SecurityException {
        Field properties = RadarPropertyHandlerImpl.class.getDeclaredField("properties");
        properties.setAccessible(true);
        properties.set(this.propertyHandler,null);
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Properties cannot be accessed without calling load() first");
        propertyHandler.getRadarProperties();
    }

    @Test
    public void loadWithInvalidFilePath() throws Exception {
        Field properties = RadarPropertyHandlerImpl.class.getDeclaredField("properties");
        properties.setAccessible(true);
        properties.set(this.propertyHandler, null);
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Config file /usr is invalid");
        String invalidPath = "/usr/";
        propertyHandler.load(invalidPath);
    }

    @Test
    public void load() throws Exception {
        Field propertiess = RadarPropertyHandlerImpl.class.getDeclaredField("properties");
        propertiess.setAccessible(true);
        propertiess.set(this.propertyHandler,null);
        propertyHandler.load("radar.yml");

        ConfigRadar properties = propertyHandler.getRadarProperties();
        assertEquals("standalone", properties.getMode());
        assertNull(properties.getLogPath());
        assertNotNull(properties.getBroker());
        assertNotNull(properties.getBrokerPaths());
        assertNotNull(properties.getReleased());
        assertNotNull(properties.getSchemaRegistry());
        assertNotNull(properties.getSchemaRegistryPaths());
        assertNotNull(properties.getZookeeper());
        assertNotNull(properties.getAutoCommitIntervalMs());
        assertNotNull(properties.getSessionTimeoutMs());
        assertNotNull(properties.getZookeeperPaths());
        assertNotNull(properties.getVersion());

    }

    @Test
    public void loadInvalidYaml() throws Exception {
        exception.expect(UnrecognizedPropertyException.class);
        propertyHandler.load("src/test/resources/config/invalidradar.yml");
    }

    @Test
    public void loadWithInstance() throws Exception {

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Properties class has been already loaded");
        propertyHandler.load("radar.yml");
        propertyHandler.load("again.yml");
        ConfigRadar propertiesS = propertyHandler.getRadarProperties();
        assertNotNull(propertiesS);
    }

    @Test
    public void loadWithLogPath() throws Exception{
        RadarPropertyHandlerImpl radarPropertyHandler = mock(RadarPropertyHandlerImpl.class);
        ConfigRadar configs = mock(ConfigRadar.class);
        when(radarPropertyHandler.getRadarProperties()).thenReturn(configs);
        when(configs.getLogPath()).thenReturn("src/test");

        doCallRealMethod().when(radarPropertyHandler).load("radar.yml");
        radarPropertyHandler.load("radar.yml");
        verify(configs, times(2)).getLogPath();
    }

    @Test
    public void loadWithInvalidLogPath() throws Exception{
        Field properties = RadarPropertyHandlerImpl.class.getDeclaredField("properties");
        properties.setAccessible(true);
        properties.set(this.propertyHandler,null);
        RadarPropertyHandlerImpl radarPropertyHandler = mock(RadarPropertyHandlerImpl.class);
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("User Log path does not exist");
        ConfigRadar configs = mock(ConfigRadar.class);
        when(radarPropertyHandler.getRadarProperties()).thenReturn(configs);
        when(configs.getLogPath()).thenReturn("hack");
        doCallRealMethod().when(radarPropertyHandler).load("radar.yml");
        radarPropertyHandler.load("radar.yml");
        verify(configs, times(2)).getLogPath();

    }

    @Test
    public void loadWithFileLogPath() throws Exception{

        RadarPropertyHandlerImpl radarPropertyHandler = mock(RadarPropertyHandlerImpl.class);
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("User Log path is not a directory");
        ConfigRadar configs = mock(ConfigRadar.class);
        when(radarPropertyHandler.getRadarProperties()).thenReturn(configs);
        when(configs.getLogPath()).thenReturn("backend.log");
        doCallRealMethod().when(radarPropertyHandler).load("radar.yml");
        radarPropertyHandler.load("radar.yml");
        verify(configs, times(2)).getLogPath();

    }

    @Test
    public void getKafkaPropertiesBeforeLoad() throws IllegalAccessException, NoSuchFieldException {
        Field properties = RadarPropertyHandlerImpl.class.getDeclaredField("properties");
        properties.setAccessible(true);
        properties.set(this.propertyHandler,null);
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Properties cannot be accessed without calling load() first");
        KafkaProperty property =propertyHandler.getKafkaProperties();
        assertNull(property);
    }

    @Test
    public void getKafkaProperties() throws Exception
    {
        Field properties = RadarPropertyHandlerImpl.class.getDeclaredField("properties");
        properties.setAccessible(true);
        properties.set(propertyHandler,null);
        propertyHandler.load("radar.yml");
        KafkaProperty property =propertyHandler.getKafkaProperties();
        assertNotNull(property);
    }

}
