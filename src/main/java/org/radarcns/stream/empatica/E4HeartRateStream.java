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

import java.io.IOException;
import javax.annotation.Nonnull;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.config.KafkaProperty;
import org.radarcns.empatica.EmpaticaE4InterBeatInterval;
import org.radarcns.key.MeasurementKey;
import org.radarcns.stream.StreamWorker;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.radarcns.util.serde.RadarSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Stream for computing and aggregating Heart Rate values collected by Empatica E4
 */
public class E4HeartRateStream extends StreamWorker<MeasurementKey, EmpaticaE4InterBeatInterval> {
    private static final Logger log = LoggerFactory.getLogger(E4HeartRateStream.class);
    private final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    public E4HeartRateStream(String clientId, int numThread, StreamMaster master,
            KafkaProperty kafkaProperties) {
        super(E4Streams.getInstance().getHeartRateStream(), clientId,
                numThread, master, kafkaProperties, log);
    }

    @Override
    protected void setStream(@Nonnull KStream<MeasurementKey, EmpaticaE4InterBeatInterval> kstream)
            throws IOException {
        kstream.groupByKey()
                .aggregate(
                    DoubleValueCollector::new,
                    (k, v, valueCollector) -> valueCollector.add(converter(v)),
                    TimeWindows.of(10 * 1000L),
                    RadarSerdes.getInstance().getDoubleCollector(),
                    getStreamDefinition().getStateStoreName())
                .toStream()
                .map(utilities::collectorToAvro)
                .to(getStreamDefinition().getOutputTopic().getName());
    }

    private double converter(EmpaticaE4InterBeatInterval record) {
        incrementMonitor();
        return utilities.ibiToHeartRate(record.getInterBeatInterval());
    }
}
