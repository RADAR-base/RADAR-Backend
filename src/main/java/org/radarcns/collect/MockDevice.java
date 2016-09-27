package org.radarcns.collect;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MockDevice extends Thread {
    private final static Logger logger = LoggerFactory.getLogger(MockDevice.class);

    private final Topic acceleration;
    private final Topic battery;
    private final Topic bvp;
    private final Topic eda;
    private final Topic ibi;
    private final Topic tags;
    private final Topic temperature;
    private final int hertz_modulus;
    private final KafkaSender<String, GenericRecord> sender;
    private final String deviceId;
    private final long nanoTimeStep;
    private long lastSleep;

    public MockDevice(KafkaSender<String, GenericRecord> sender, String deviceId) {
        this.deviceId = deviceId;
        acceleration = new Topic("empatica_e4_acceleration", 32);
        battery = new Topic("empatica_e4_battery_level", 1);
        bvp = new Topic("empatica_e4_blood_volume_pulse", 64);
        eda = new Topic("empatica_e4_electrodermal_activity", 4);
        ibi = new Topic("empatica_e4_inter_beat_interval", 1);
        tags = new Topic("empatica_e4_tags", 1);
        temperature = new Topic("empatica_e4_temperature", 4);
        hertz_modulus = 64;
        nanoTimeStep = 1000000000L / hertz_modulus;
        lastSleep = 0;
        this.sender = sender;
    }

    public void run() {
        lastSleep = System.nanoTime();
        try {
            while (true) {
                for (int i = 0; i < hertz_modulus; i++) {
                    sendIfNeeded(i, acceleration,
                            "x", fixedValue(acceleration, "x", (byte) 15),
                            "y", fixedValue(acceleration, "y", (byte) -15),
                            "z", fixedValue(acceleration, "z", (byte) 64));
                    sendIfNeeded(i, battery, "batteryLevel", 0.8f);
                    sendIfNeeded(i, bvp, "bloodVolumePulse", 80.0f);
                    sendIfNeeded(i, eda, "electroDermalActivity", 0.026897f);
                    sendIfNeeded(i, ibi, "interBeatInterval", 0.921917f);
                    sendIfNeeded(i, tags);
                    sendIfNeeded(i, temperature, "temperature", 37.0f);
                    sleep();
                }
            }
        } catch (InterruptedException ex) {
            // do nothing, just exit the loop
        }
    }

    private GenericFixed fixedValue(Topic t, String field, byte b) {
        return new GenericData.Fixed(acceleration.getSchema().getField("x").schema(), new byte[]{b});
    }

    private void sendIfNeeded(int timeStep, Topic topic, Object... values) {
        if (topic.getHertz() > 0 && timeStep % (hertz_modulus / topic.getHertz()) == 0) {
            GenericRecord avroRecord = new GenericData.Record(topic.getSchema());
            avroRecord.put("time", System.currentTimeMillis() / 1000.0);
            for (int i = 0; i < values.length; i += 2) {
                avroRecord.put((String) values[i], values[i + 1]);
            }
            sender.send(topic.getName(), deviceId, avroRecord);
        }
    }

    public void waitFor() throws InterruptedException {
        while (isAlive()) {
            join();
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
}
