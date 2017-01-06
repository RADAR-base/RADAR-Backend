package org.radarcns.empatica.topic;

import org.apache.avro.specific.SpecificRecord;
import org.radarcns.empatica.EmpaticaE4Acceleration;
import org.radarcns.empatica.EmpaticaE4BatteryLevel;
import org.radarcns.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.empatica.EmpaticaE4ElectroDermalActivity;
import org.radarcns.empatica.EmpaticaE4InterBeatInterval;
import org.radarcns.empatica.EmpaticaE4SensorStatus;
import org.radarcns.empatica.EmpaticaE4Temperature;
import org.radarcns.topic.SensorTopic;
import org.radarcns.topic.SensorTopics;

import java.util.HashSet;
import java.util.Set;

/**
 * Entire set of Empatica E4 SensorTopic
 * @see SensorTopic
 */
public final class E4SensorTopics implements SensorTopics {

    //All sensor topics
    private final SensorTopic<EmpaticaE4Acceleration> accelerationTopic;
    private final SensorTopic<EmpaticaE4BatteryLevel> batteryLevelTopic;
    private final SensorTopic<EmpaticaE4BloodVolumePulse> bloodVolumePulseTopic;
    private final SensorTopic<EmpaticaE4ElectroDermalActivity> electroDermalActivityTopic;
    private final SensorTopic<EmpaticaE4InterBeatInterval> interBeatIntervalTopic;
    private final SensorTopic<EmpaticaE4SensorStatus> sensorStatusTopic;
    private final SensorTopic<EmpaticaE4Temperature> temperatureTopic;

    private static E4SensorTopics instance = new E4SensorTopics();

    static E4SensorTopics getInstance() {
        return instance;
    }

    private E4SensorTopics() {
        accelerationTopic = new SensorTopic<>(
                "android_empatica_e4_acceleration");
        batteryLevelTopic = new SensorTopic<>(
                "android_empatica_e4_battery_level");
        bloodVolumePulseTopic = new SensorTopic<>(
                "android_empatica_e4_blood_volume_pulse");
        electroDermalActivityTopic = new SensorTopic<>(
                "android_empatica_e4_electrodermal_activity");
        interBeatIntervalTopic = new SensorTopic<>(
                "android_empatica_e4_inter_beat_interval");
        sensorStatusTopic = new SensorTopic<>(
                "android_empatica_e4_sensor_status");
        temperatureTopic = new SensorTopic<>(
                "android_empatica_e4_temperature");
    }

    @Override
    public SensorTopic<? extends SpecificRecord> getTopic(String name) {
        switch (name) {
            case "android_empatica_e4_acceleration":
                return accelerationTopic;
            case "android_empatica_e4_battery_level":
                return batteryLevelTopic;
            case "android_empatica_e4_blood_volume_pulse":
                return bloodVolumePulseTopic;
            case "android_empatica_e4_electrodermal_activity":
                return electroDermalActivityTopic;
            case "android_empatica_e4_inter_beat_interval":
                return interBeatIntervalTopic;
            case "android_empatica_e4_sensor_status":
                return sensorStatusTopic;
            case "android_empatica_e4_temperature":
                return temperatureTopic;
            default:
                throw new IllegalArgumentException("Topic " + name + " unknown");
        }
    }

    @Override
    public Set<String> getTopicNames() {
        Set<String> set = new HashSet<>();

        set.addAll(accelerationTopic.getAllTopicNames());
        set.addAll(batteryLevelTopic.getAllTopicNames());
        set.addAll(bloodVolumePulseTopic.getAllTopicNames());
        set.addAll(electroDermalActivityTopic.getAllTopicNames());
        set.addAll(interBeatIntervalTopic.getAllTopicNames());
        set.addAll(sensorStatusTopic.getAllTopicNames());
        set.addAll(temperatureTopic.getAllTopicNames());

        return set;
    }

    public SensorTopic<EmpaticaE4Acceleration> getAccelerationTopic() {
        return accelerationTopic;
    }

    public SensorTopic<EmpaticaE4BatteryLevel> getBatteryLevelTopic() {
        return batteryLevelTopic;
    }

    public SensorTopic<EmpaticaE4BloodVolumePulse> getBloodVolumePulseTopic() {
        return bloodVolumePulseTopic;
    }

    public SensorTopic<EmpaticaE4ElectroDermalActivity> getElectroDermalActivityTopic() {
        return electroDermalActivityTopic;
    }

    public SensorTopic<EmpaticaE4InterBeatInterval> getInterBeatIntervalTopic() {
        return interBeatIntervalTopic;
    }

    public SensorTopic<EmpaticaE4SensorStatus> getSensorStatusTopic() {
        return sensorStatusTopic;
    }

    public SensorTopic<EmpaticaE4Temperature> getTemperatureTopic() {
        return temperatureTopic;
    }

}
