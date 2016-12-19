package org.radarcns.config;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Created by nivethika on 19-12-16.
 */
public class PropertiesRadarTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void load() throws Exception {

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Property cannot be accessed without calling load() first");
        PropertiesRadar.getInstance();

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Config file does not exist");
        PropertiesRadar.load(null);

        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Config file is invalid");
        String invalidPath = "/usr/";
        PropertiesRadar.load(invalidPath);

        PropertiesRadar.load("radar.yml");
    }



}
