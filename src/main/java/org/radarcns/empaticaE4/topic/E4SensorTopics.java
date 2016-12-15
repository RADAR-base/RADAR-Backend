package org.radarcns.empaticaE4.topic;

import org.apache.avro.specific.SpecificRecord;
import org.radarcns.empaticaE4.EmpaticaE4Acceleration;
import org.radarcns.empaticaE4.EmpaticaE4BatteryLevel;
import org.radarcns.empaticaE4.EmpaticaE4BloodVolumePulse;
import org.radarcns.empaticaE4.EmpaticaE4ElectroDermalActivity;
import org.radarcns.empaticaE4.EmpaticaE4InterBeatInterval;
import org.radarcns.empaticaE4.EmpaticaE4SensorStatus;
import org.radarcns.empaticaE4.EmpaticaE4Temperature;
import org.radarcns.topic.sensor.SensorTopic;
import org.radarcns.topic.sensor.SensorTopics;

import java.util.HashSet;
import java.util.Set;

/**
 * Entire set of Empatica E4 SensorTopic
 * @see org.radarcns.topic.sensor.SensorTopic
 */
public class E4SensorTopics implements SensorTopics {

    //All sensor topics
    private final SensorTopic<EmpaticaE4Acceleration> accelerationTopic;
    private final SensorTopic<EmpaticaE4BatteryLevel> batteryLevelTopic;
    private final SensorTopic<EmpaticaE4BloodVolumePulse> bloodVolumePulseTopic;
    private final SensorTopic<EmpaticaE4ElectroDermalActivity> electroDermalActivityTopic;
    private final SensorTopic<EmpaticaE4InterBeatInterval> interBeatIntervalTopic;
    private final SensorTopic<EmpaticaE4SensorStatus> sensorStatusTopic;
    private final SensorTopic<EmpaticaE4Temperature> temperatureTopic;

    private static E4SensorTopics instance = new E4SensorTopics();

    protected static E4SensorTopics getInstance() {
        return instance;
    }

    private E4SensorTopics() {
        accelerationTopic = new SensorTopic<>(
                "android_empatica_e4_acceleration", EmpaticaE4Acceleration.class);
        batteryLevelTopic = new SensorTopic<>(
                "android_empatica_e4_battery_level", EmpaticaE4BatteryLevel.class);
        bloodVolumePulseTopic = new SensorTopic<>(
                "android_empatica_e4_blood_volume_pulse", EmpaticaE4BloodVolumePulse.class);
        electroDermalActivityTopic = new SensorTopic<>(
                "android_empatica_e4_electrodermal_activity", EmpaticaE4ElectroDermalActivity.class);
        interBeatIntervalTopic = new SensorTopic<>(
                "android_empatica_e4_inter_beat_interval", EmpaticaE4InterBeatInterval.class);
        sensorStatusTopic = new SensorTopic<>(
                "android_empatica_e4_sensor_status", EmpaticaE4SensorStatus.class);
        temperatureTopic = new SensorTopic<>(
                "android_empatica_e4_temperature", EmpaticaE4Temperature.class);
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
