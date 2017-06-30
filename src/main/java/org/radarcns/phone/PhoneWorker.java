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

package org.radarcns.phone;

import org.radarcns.config.KafkaProperty;
import org.radarcns.config.RadarPropertyHandler;
import org.radarcns.phone.streams.PhoneUsage;
import org.radarcns.phone.topic.PhoneStreams;
import org.radarcns.stream.aggregator.AggregatorWorker;
import org.radarcns.stream.aggregator.MasterAggregator;
import org.radarcns.util.RadarSingletonFactory;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Singleton MasterAggregator for Phone
 * @see org.radarcns.stream.aggregator.MasterAggregator
 */
public class PhoneWorker extends MasterAggregator {

    public PhoneWorker(boolean standalone) throws IOException {
        super(standalone,"Phone");
    }

    @Override
    protected void announceTopics(@Nonnull Logger log) {
        log.info("If AUTO.CREATE.TOPICS.ENABLE is FALSE you must create the following topics "
                        + "before starting: \n  - {}",
                String.join("\n  - ", PhoneStreams.getInstance().getTopicNames()));
    }

    @Override
    protected List<AggregatorWorker<?,?>> createWorkers(int low, int normal, int high) {
        RadarPropertyHandler propertyHandler = RadarSingletonFactory.getRadarPropertyHandler();
        KafkaProperty kafkaProperty = propertyHandler.getKafkaProperties();
        return Arrays.asList(
                new PhoneUsage(
                        "PhoneUsage", low, this, kafkaProperty)
        );
    }
}
