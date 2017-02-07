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

package org.radarcns.empatica.streams;

import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.config.KafkaProperty;
import org.radarcns.empatica.EmpaticaE4Acceleration;
import org.radarcns.empatica.topic.E4Streams;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.aggregator.AggregatorWorker;
import org.radarcns.stream.aggregator.MasterAggregator;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.radarcns.util.serde.RadarSerdes;

/**
 * Definition of Kafka Stream for aggregating data collected by Empatica E4 Acceleromete
 */
public class E4Acceleration extends AggregatorWorker<MeasurementKey, EmpaticaE4Acceleration> {
    private final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    public E4Acceleration(String clientId, int numThread, MasterAggregator master,
            KafkaProperty kafkaProperties) {
        super(E4Streams.getInstance().getSensorStreams().getAccelerationStream(), clientId,
                numThread, master, kafkaProperties);
    }


    @Override
    protected void setStream(@Nonnull KStream<MeasurementKey, EmpaticaE4Acceleration> kstream)
            throws IOException {
        kstream.groupByKey()
            .aggregate(
                DoubleArrayCollector::new,
                (k, v, valueCollector) -> valueCollector.add(utilities.accelerationToArray(v)),
                TimeWindows.of(10 * 1000L),
                RadarSerdes.getInstance().getDoubelArrayCollector(),
                getStreamDefinition().getStateStoreName())
            .toStream()
            .map((k, v) -> new KeyValue<>(utilities.getWindowed(k), v.convertToAvro()))
            .to(getStreamDefinition().getOutputTopic().getName());
    }
}
