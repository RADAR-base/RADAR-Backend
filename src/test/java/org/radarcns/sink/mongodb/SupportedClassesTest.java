package org.radarcns.sink.mongodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Collection;
import org.junit.Test;
import org.radarcns.application.ApplicationUptime;
import org.radarcns.key.MeasurementKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by francesco on 27/02/2017.
 */
public class SupportedClassesTest {

    private static final Logger log = LoggerFactory.getLogger(SupportedClassesTest.class);

    @Test
    public void testClassName() {
        assertEquals("org.radarcns.key.MeasurementKey", MeasurementKey.class.getCanonicalName());
        assertNotEquals("org.radarcns.key.MeasurementKey", ApplicationUptime.class.getCanonicalName());
        assertEquals("org.radarcns.application.ApplicationUptime", ApplicationUptime.class.getCanonicalName());

        Collection<String> supported = new UptimeStatusRecordConverter().supportedSchemaNames();
        String[] supportedClasses = supported.toArray(new String[supported.size()]);

        assertEquals("org.radarcns.key.MeasurementKey-org.radarcns.application.ApplicationUptime", supportedClasses[0]);
        assertNotEquals("org.radarcns.key.MeasurementKey org.radarcns.application.ApplicationUptime", supportedClasses[0]);
    }
}
