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

package org.radarcns.phone.streams;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.config.KafkaProperty;
import org.radarcns.empatica.topic.E4Streams;
import org.radarcns.key.MeasurementKey;
import org.radarcns.phone.PhoneUsageEvent;
import org.radarcns.stream.aggregator.AggregatorWorker;
import org.radarcns.stream.aggregator.MasterAggregator;
import org.radarcns.stream.collector.DoubleValueCollector;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.radarcns.util.serde.RadarSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;

public class PhoneUsage extends AggregatorWorker<MeasurementKey, PhoneUsageEvent> {
    private static final Logger log = LoggerFactory.getLogger(PhoneUsage.class);
    private final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    public PhoneUsage(String clientId, int numThread, MasterAggregator master,
                       KafkaProperty kafkaProperties) {
        super(E4Streams.getInstance().getHeartRateStream(), clientId,
                numThread, master, kafkaProperties, log);
    }

    @Override
    protected void setStream(@Nonnull KStream<MeasurementKey, PhoneUsageEvent> kstream)
            throws IOException {
        // TODO
    }

}
