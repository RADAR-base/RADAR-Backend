package org.radarcns.util;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.Before;
import org.junit.Test;
import org.radarcns.empatica.EmpaticaE4Acceleration;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;

import static org.junit.Assert.assertEquals;

public class RadarUtilsTest {

    private RadarUtilities radarUtilities;

    @Before
    public void setUp() {
        this.radarUtilities = new RadarUtilitiesImpl();
    }

    @Test
    public void getWindowed() {
        String userId = "userId";
        MeasurementKey measurementKey = new MeasurementKey();
        measurementKey.setUserId(userId);
        Window window = new TimeWindow(1, 4);
        Windowed<MeasurementKey> measurementKeyWindowed = new Windowed<>(measurementKey, window);

        WindowedKey windowedKey = radarUtilities.getWindowed(measurementKeyWindowed);
        assertEquals(windowedKey.getUserID(), userId);
    }

    @Test
    public void floatToDouble() {
        double value = radarUtilities.floatToDouble(1.0f);
        assertEquals(value, 1.0d, 0.0d);
    }

    @Test
    public void ibiToHR() {
        double hR = radarUtilities.ibiToHR(1.0f);
        assertEquals((60d / 1.0d), hR, 0.0d);
    }

    @Test
    public void accelerationToArray() {
        EmpaticaE4Acceleration acceleration = new EmpaticaE4Acceleration();
        acceleration.setX(1.0f);
        acceleration.setY(2.0f);
        acceleration.setZ(3.0f);

        double[] value = radarUtilities.accelerationToArray(acceleration);
        assertEquals(value[0], 1.0f, 0.0);
        assertEquals(value[1], 2.0f, 0.0);
        assertEquals(value[2], 3.0f, 0.0);
    }

}
