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

package org.radarcns.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import java.io.IOException;
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
    public void setUp() {
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
        String invalidPath = "/usr/";
        assertThrows(IOException.class, () -> propertyHandler.load(invalidPath));
    }

    @Test
    public void load() throws Exception {
        Field propertiess = RadarPropertyHandlerImpl.class.getDeclaredField("properties");
        propertiess.setAccessible(true);
        propertiess.set(this.propertyHandler,null);
        propertyHandler.load("src/test/resources/config/radar.yml");

        ConfigRadar properties = propertyHandler.getRadarProperties();
        assertNotNull(properties.getBroker());
        assertNotNull(properties.getBrokerPaths());
        assertNotNull(properties.getReleased());
        assertNotNull(properties.getSchemaRegistry());
        assertNotNull(properties.getSchemaRegistryPaths());
        assertNotNull(properties.getZookeeper());
        assertNotNull(properties.getZookeeperPaths());
        assertNotNull(properties.getVersion());
        assertThat(properties.getExtras(), hasEntry("somethingother", "bla"));
    }

    @Test
    public void loadInvalidYaml() throws Exception {
        exception.expect(UnrecognizedPropertyException.class);
        propertyHandler.load("src/test/resources/config/invalidradar.yml");
    }

    @Test
    public void loadInvalidStreamPriority() throws Exception {
        exception.expect(JsonMappingException.class);
        propertyHandler.load("src/test/resources/config/invalid_stream_priority.yml");
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
    public void getKafkaProperties() throws Exception {
        Field properties = RadarPropertyHandlerImpl.class.getDeclaredField("properties");
        properties.setAccessible(true);
        properties.set(propertyHandler,null);
        propertyHandler.load("radar.yml");
        KafkaProperty property = propertyHandler.getKafkaProperties();
        assertNotNull(property);
    }

}
