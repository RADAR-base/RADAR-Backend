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

import javax.annotation.Nonnull;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.stream.aggregator.DoubleArrayAggregation;
import org.radarcns.config.KafkaProperty;
import org.radarcns.passive.empatica.EmpaticaE4Acceleration;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.kafka.AggregateKey;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.radarcns.util.serde.RadarSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Definition of Kafka Stream for aggregating data collected by Empatica E4 Accelerometer sensor.
 */
public class E4AccelerationStream extends StreamWorker<ObservationKey, EmpaticaE4Acceleration> {
    private static final Logger log = LoggerFactory.getLogger(E4AccelerationStream.class);
    private final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    public E4AccelerationStream(String clientId, int numThread, StreamMaster master,
            KafkaProperty kafkaProperties) {
        super(E4Streams.getInstance().getAccelerationStream(), clientId,
                numThread, master, kafkaProperties, log);
    }

    @Override
    protected KStream<AggregateKey, DoubleArrayAggregation> defineStream(
            @Nonnull KStream<ObservationKey, EmpaticaE4Acceleration> kstream) {
        return kstream.groupByKey()
            .aggregate(
                DoubleArrayCollector::new,
                (k, v, valueCollector) -> valueCollector.add(utilities.accelerationToArray(v)),
                TimeWindows.of(10 * 1000L),
                RadarSerdes.getInstance().getDoubleArrayCollector(),
                getStreamDefinition().getStateStoreName())
            .toStream()
            .map(utilities::collectorToAvro);
    }
}
