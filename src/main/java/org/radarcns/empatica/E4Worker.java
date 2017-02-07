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

package org.radarcns.empatica;

import java.util.Arrays;
import org.radarcns.config.KafkaProperty;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.empatica.streams.E4Acceleration;
import org.radarcns.empatica.streams.E4BatteryLevel;
import org.radarcns.empatica.streams.E4BloodVolumePulse;
import org.radarcns.empatica.streams.E4ElectroDermalActivity;
import org.radarcns.empatica.streams.E4HeartRate;
import org.radarcns.empatica.streams.E4InterBeatInterval;
import org.radarcns.empatica.streams.E4Temperature;
import org.radarcns.empatica.topic.E4Streams;
import org.radarcns.stream.aggregator.AggregatorWorker;
import org.radarcns.stream.aggregator.MasterAggregator;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

/**
 * Singleton MasterAggregator for Empatica E4
 * @see org.radarcns.stream.aggregator.MasterAggregator
 */
public class E4Worker extends MasterAggregator {

    public E4Worker(boolean standalone) throws IOException {
        super(standalone,"Empatica E4");
    }

    @Override
    protected void announceTopics(@Nonnull Logger log) {
        log.info("If AUTO.CREATE.TOPICS.ENABLE is FALSE you must create the following topics "
                + "before starting: \n  - {}",
                String.join("\n  - ", E4Streams.getInstance().getTopicNames()));
    }

    @Override
    protected List<AggregatorWorker<?,?>> createWorkers(int low, int normal, int high) {
        RadarPropertyHandler propertyHandler = RadarSingletonFactory.getRadarPropertyHandler();
        KafkaProperty kafkaProperty = propertyHandler.getKafkaProperties();
        return Arrays.asList(
                new E4Acceleration(
                        "E4Acceleration", high, this, kafkaProperty),
                new E4BatteryLevel(
                        "E4BatteryLevel", low, this, kafkaProperty),
                new E4BloodVolumePulse(
                        "E4BloodVolumePulse", high, this, kafkaProperty),
                new E4ElectroDermalActivity(
                        "E4ElectroDermalActivity", normal, this, kafkaProperty),
                new E4HeartRate(
                        "E4HeartRate", high,this, kafkaProperty),
                new E4InterBeatInterval(
                        "E4InterBeatInterval", high,this, kafkaProperty),
                new E4Temperature(
                        "E4Temperature", high, this, kafkaProperty));
    }
}
