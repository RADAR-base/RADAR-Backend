package org.radarcns.util;

import org.apache.kafka.streams.kstream.Windowed;
import org.radarcns.empaticaE4.EmpaticaE4Acceleration;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;

/**
 * Created by nivethika on 20-12-16.
 */
public interface RadarUtilities {
    WindowedKey getWindowed(Windowed<MeasurementKey> window);

    double floatToDouble(float input);

    double ibiToHR(float input);

    double[] accelerationToArray(EmpaticaE4Acceleration value);
}
