package org.radarcns.producer;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.radarcns.empatica.EmpaticaE4Acceleration;
import org.radarcns.empatica.EmpaticaE4BatteryLevel;
import org.radarcns.empatica.EmpaticaE4BloodVolumePulse;
import org.radarcns.empatica.EmpaticaE4ElectroDermalActivity;
import org.radarcns.empatica.EmpaticaE4InterBeatInterval;
import org.radarcns.empatica.EmpaticaE4Tag;
import org.radarcns.empatica.EmpaticaE4Temperature;
import org.radarcns.topic.AvroTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class MockDevice<K> extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(MockDevice.class);
    private static final AtomicLong OFFSET = new AtomicLong(0);

    private final AvroTopic<K, EmpaticaE4Acceleration> acceleration;
    private final AvroTopic<K, EmpaticaE4BatteryLevel> battery;
    private final AvroTopic<K, EmpaticaE4BloodVolumePulse> bvp;
    private final AvroTopic<K, EmpaticaE4ElectroDermalActivity> eda;
    private final AvroTopic<K, EmpaticaE4InterBeatInterval> ibi;
    private final AvroTopic<K, EmpaticaE4Tag> tags;
    private final AvroTopic<K, EmpaticaE4Temperature> temperature;
    private final int hertzModulus;
    private final KafkaSender<K, SpecificRecord> sender;
    private final K key;
    private final long nanoTimeStep;
    private final float batteryDecayFactor;
    private final float timeDriftFactor;
    private final AtomicBoolean stopping;

    private long lastSleep;
    private IOException exception;

    public MockDevice(KafkaSender<K, SpecificRecord> sender, K key,
            Schema keySchema, Class<K> keyClass) {
        this.key = key;
        acceleration = new AvroTopic<>("backend_mock_empatica_e4_acceleration",
                keySchema, EmpaticaE4Acceleration.getClassSchema(),
                keyClass, EmpaticaE4Acceleration.class);
        battery = new AvroTopic<>("backend_mock_empatica_e4_battery_level",
                keySchema, EmpaticaE4BatteryLevel.getClassSchema(),
                keyClass, EmpaticaE4BatteryLevel.class);
        bvp = new AvroTopic<>("backend_mock_empatica_e4_blood_volume_pulse",
                keySchema, EmpaticaE4BloodVolumePulse.getClassSchema(),
                keyClass, EmpaticaE4BloodVolumePulse.class);
        eda = new AvroTopic<>("backend_mock_empatica_e4_electrodermal_activity",
                keySchema, EmpaticaE4ElectroDermalActivity.getClassSchema(),
                keyClass, EmpaticaE4ElectroDermalActivity.class);
        ibi = new AvroTopic<>("backend_mock_empatica_e4_inter_beat_interval",
                keySchema, EmpaticaE4InterBeatInterval.getClassSchema(),
                keyClass, EmpaticaE4InterBeatInterval.class);
        tags = new AvroTopic<>("backend_mock_empatica_e4_tag",
                keySchema, EmpaticaE4Tag.getClassSchema(),
                keyClass, EmpaticaE4Tag.class);
        temperature = new AvroTopic<>("backend_mock_empatica_e4_temperature",
                keySchema, EmpaticaE4Temperature.getClassSchema(),
                keyClass, EmpaticaE4Temperature.class);
        hertzModulus = 64;
        nanoTimeStep = 1000000000L / hertzModulus;
        lastSleep = 0;


        // decay
        Random random = new Random();
        batteryDecayFactor = 0.1f * random.nextFloat();
        timeDriftFactor = 0.01f * random.nextFloat();

        this.sender = sender;
        this.stopping = new AtomicBoolean(false);
        exception = null;
    }

    public void run() {
        lastSleep = System.nanoTime();

        try (
                KafkaTopicSender<K, EmpaticaE4Acceleration> accelerationSender
                        = sender.sender(acceleration);
                KafkaTopicSender<K, EmpaticaE4BatteryLevel> batterySender
                        = sender.sender(battery);
                KafkaTopicSender<K, EmpaticaE4BloodVolumePulse> bvpSender
                        = sender.sender(bvp);
                KafkaTopicSender<K, EmpaticaE4ElectroDermalActivity> edaSender
                        = sender.sender(eda);
                KafkaTopicSender<K, EmpaticaE4InterBeatInterval> ibiSender
                        = sender.sender(ibi);
                KafkaTopicSender<K, EmpaticaE4Tag> tagSender
                        = sender.sender(tags);
                KafkaTopicSender<K, EmpaticaE4Temperature> temperatureSender
                        = sender.sender(temperature)) {
            int accelerationFrequency = 32;
            int batteryFrequency = 1;
            int bvpFrequency = 64;
            int edaFrequency = 4;
            int ibiFrequency = 1;
            int tagsFrequency = 1;
            int temperatureFrequency = 4;

            for (int t = 0; t < Integer.MAX_VALUE && !stopping.get(); t++) {
                for (int i = 1; i <= hertzModulus; i++) {
                    double timeReceived = System.currentTimeMillis() / 1000d;
                    double time = timeReceived + t * timeDriftFactor;
                    sendIfNeeded(i, accelerationFrequency, accelerationSender,
                            new EmpaticaE4Acceleration(time, timeReceived, 15f, -15f, 64f));
                    sendIfNeeded(i, batteryFrequency, batterySender,
                            new EmpaticaE4BatteryLevel(time, timeReceived,
                                    1f - (batteryDecayFactor * t % 1)));
                    sendIfNeeded(i, bvpFrequency, bvpSender,
                            new EmpaticaE4BloodVolumePulse(time, timeReceived, 80.0f));
                    sendIfNeeded(i, edaFrequency, edaSender,
                            new EmpaticaE4ElectroDermalActivity(time, timeReceived, 0.026897f));
                    sendIfNeeded(i, ibiFrequency, ibiSender,
                            new EmpaticaE4InterBeatInterval(time, timeReceived, 0.921917f));
                    sendIfNeeded(i, tagsFrequency, tagSender,
                            new EmpaticaE4Tag(time, timeReceived));
                    sendIfNeeded(i, temperatureFrequency, temperatureSender,
                            new EmpaticaE4Temperature(time, timeReceived, 37.0f));
                    sleep();
                }
                logger.debug("Single time step {}", key);
            }
        } catch (InterruptedException ex) {
            // do nothing, just exit the loop
        } catch (IOException e) {
            synchronized (this) {
                this.exception = e;
            }
            logger.error("MockDevice {} failed to send message", key, e);
        }
    }

    private <V extends SpecificRecord> void sendIfNeeded(int timeStep, int frequency,
            KafkaTopicSender<K, V> topicSender, V avroRecord) throws IOException {
        if (frequency > 0 && timeStep % (hertzModulus / frequency) == 0) {
            synchronized (OFFSET) {
                topicSender.send(OFFSET.incrementAndGet(), key, avroRecord);
            }
        }
    }

    private void sleep() throws InterruptedException {
        long currentTime = System.nanoTime();
        long nanoToSleep = nanoTimeStep - currentTime + lastSleep;
        if (nanoToSleep > 0) {
            Thread.sleep(nanoToSleep / 1000000L, ((int) nanoToSleep) % 1000000);
        }
        lastSleep = currentTime;
    }

    public void shutdown() {
        stopping.set(true);
    }

    public synchronized IOException getException() {
        return exception;
    }
}
