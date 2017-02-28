/*
 * Copyright 2017 Kings College London and The Hyve
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

package org.radarcns.empatica.topic;


import org.radarcns.stream.GeneralStreamGroup;
import org.radarcns.stream.StreamDefinition;

/**
 * Singleton class representing the list of Empatica E4 topics
 */
public final class E4Streams extends GeneralStreamGroup {
    private static final E4Streams INSTANCE = new E4Streams();

    //All sensor topics
    private final StreamDefinition accelerationStream;
    private final StreamDefinition batteryLevelStream;
    private final StreamDefinition bloodVolumePulseStream;
    private final StreamDefinition electroDermalActivityStream;
    private final StreamDefinition interBeatIntervalStream;
    private final StreamDefinition sensorStatusStream;
    private final StreamDefinition temperatureStream;

    // Internal topics
    private final StreamDefinition heartRateStream;

    public static E4Streams getInstance() {
        return INSTANCE;
    }

    private E4Streams() {
        accelerationStream = createSensorStream(
                "android_empatica_e4_acceleration");
        batteryLevelStream = createSensorStream(
                "android_empatica_e4_battery_level");
        bloodVolumePulseStream = createSensorStream(
                "android_empatica_e4_blood_volume_pulse");
        electroDermalActivityStream = createSensorStream(
                "android_empatica_e4_electrodermal_activity");
        interBeatIntervalStream = createSensorStream(
                "android_empatica_e4_inter_beat_interval");
        sensorStatusStream = createSensorStream(
                "android_empatica_e4_sensor_status");
        temperatureStream = createSensorStream(
                "android_empatica_e4_temperature");

        heartRateStream = createStream(
                "android_empatica_e4_inter_beat_interval",
                "android_empatica_e4_heartrate_output");
    }

    public StreamDefinition getAccelerationStream() {
        return accelerationStream;
    }

    public StreamDefinition getBatteryLevelStream() {
        return batteryLevelStream;
    }

    public StreamDefinition getBloodVolumePulseStream() {
        return bloodVolumePulseStream;
    }

    public StreamDefinition getElectroDermalActivityStream() {
        return electroDermalActivityStream;
    }

    public StreamDefinition getInterBeatIntervalStream() {
        return interBeatIntervalStream;
    }

    public StreamDefinition getSensorStatusStream() {
        return sensorStatusStream;
    }

    public StreamDefinition getTemperatureStream() {
        return temperatureStream;
    }

    public StreamDefinition getHeartRateStream() {
        return heartRateStream;
    }
}
