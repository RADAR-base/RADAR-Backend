package org.radarcns.util;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;
import org.radarcns.empatica.EmpaticaE4Acceleration;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;

/**
 * Implements {@link RadarUtilities}
 */
public class RadarUtilitiesImpl implements RadarUtilities {
    protected RadarUtilitiesImpl() {
        // used for construction from RadarSingletonFactory
    }

    public WindowedKey getWindowed(Windowed<MeasurementKey> window) {
        return new WindowedKey(window.key().getUserId(), window.key().getSourceId(),
                window.window().start(), window.window().end());
    }

    public double floatToDouble(float input) {
        return Double.parseDouble(String.valueOf(input));
    }

    public double ibiToHeartRate(float input) {
        return 60d / floatToDouble(input);
    }

    public double[] accelerationToArray(EmpaticaE4Acceleration value) {
        return new double[] {
                floatToDouble(value.getX()),
                floatToDouble(value.getY()),
                floatToDouble(value.getZ())};
    }

}
