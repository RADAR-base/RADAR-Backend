package org.radarcns.util;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Windowed;
import org.radarcns.empatica.EmpaticaE4Acceleration;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;

/**
 * Interface that facades all utility functions that are required to support RadarBackend features
 */
public interface RadarUtilities {

    /**
     * Creates a WindowedKey for a window of MeasurementKey
     * @param window Windowed measurement keys
     * @return relevant WindowedKey
     */
    WindowedKey getWindowed(Windowed<MeasurementKey> window);


    double floatToDouble(float input);

    double ibiToHeartRate(float input);

    double[] accelerationToArray(EmpaticaE4Acceleration value);

    /**
     * Converts a MesurementKey Struct into a String
     * @param key record key
     * @return converted key string
     */
    String measurementKeyToMongoDbKey(Struct key);
}
