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

package org.radarcns.stream.empatica;


import java.util.Collection;
import org.radarcns.stream.GeneralStreamGroup;
import org.radarcns.stream.StreamDefinition;

/**
 * Singleton class representing the list of Empatica E4 topics
 */
public final class E4Streams extends GeneralStreamGroup {
    private static final E4Streams INSTANCE = new E4Streams();

    //All sensor topics
    private final Collection<StreamDefinition> accelerationStream;
    private final Collection<StreamDefinition> batteryLevelStream;
    private final Collection<StreamDefinition> bloodVolumePulseStream;
    private final Collection<StreamDefinition> electroDermalActivityStream;
    private final Collection<StreamDefinition> interBeatIntervalStream;
    private final Collection<StreamDefinition> temperatureStream;

    // Internal topics
    private final Collection<StreamDefinition> heartRateStream;

    public static E4Streams getInstance() {
        return INSTANCE;
    }

    private E4Streams() {
        accelerationStream = createWindowedSensorStream(
                "android_empatica_e4_acceleration");
        batteryLevelStream = createWindowedSensorStream(
                "android_empatica_e4_battery_level");
        bloodVolumePulseStream = createWindowedSensorStream(
                "android_empatica_e4_blood_volume_pulse");
        electroDermalActivityStream = createWindowedSensorStream(
                "android_empatica_e4_electrodermal_activity");
        interBeatIntervalStream = createWindowedSensorStream(
                "android_empatica_e4_inter_beat_interval");
        temperatureStream = createWindowedSensorStream(
                "android_empatica_e4_temperature");

        heartRateStream = createWindowedSensorStream(
                "android_empatica_e4_inter_beat_interval",
                "android_empatica_e4_heart_rate");
    }

    public Collection<StreamDefinition> getAccelerationStream() {
        return accelerationStream;
    }

    public Collection<StreamDefinition> getBatteryLevelStream() {
        return batteryLevelStream;
    }

    public Collection<StreamDefinition> getBloodVolumePulseStream() {
        return bloodVolumePulseStream;
    }

    public Collection<StreamDefinition> getElectroDermalActivityStream() {
        return electroDermalActivityStream;
    }

    public Collection<StreamDefinition> getInterBeatIntervalStream() {
        return interBeatIntervalStream;
    }

    public Collection<StreamDefinition> getTemperatureStream() {
        return temperatureStream;
    }

    public Collection<StreamDefinition> getHeartRateStream() {
        return heartRateStream;
    }
}
