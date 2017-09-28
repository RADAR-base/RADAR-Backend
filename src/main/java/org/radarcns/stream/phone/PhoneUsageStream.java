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

package org.radarcns.stream.phone;

import javax.annotation.Nonnull;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.KafkaProperty;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneUsageEvent;
import org.radarcns.stream.StreamDefinition;
import org.radarcns.stream.StreamMaster;
import org.radarcns.stream.StreamWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class PhoneUsageStream extends StreamWorker<ObservationKey, PhoneUsageEvent> {
    private static final Logger logger = LoggerFactory.getLogger(PhoneUsageStream.class);

    // 1 day until an item is refreshed
    private static final int CACHE_TIMEOUT = 24 * 3600;

    // Do not cache more than 1 million elements, for memory consumption reasons
    private static final int MAX_CACHE_SIZE = 1_000_000;

    private final PlayStoreLookup playStoreLookup;

    public PhoneUsageStream(Collection<StreamDefinition> definitions, int numThread,
            StreamMaster master, KafkaProperty kafkaProperties) {
        super(definitions, numThread, master, kafkaProperties, logger);
        playStoreLookup =  new PlayStoreLookup(CACHE_TIMEOUT, MAX_CACHE_SIZE);
    }

    @Override
    protected KStream<ObservationKey, PhoneUsageEvent> implementStream(StreamDefinition definition,
            @Nonnull KStream<ObservationKey, PhoneUsageEvent> kstream) {
        return kstream
            .map((key, value) -> {
                String packageName = value.getPackageName();
                PlayStoreLookup.AppCategory category = playStoreLookup.lookupCategory(packageName);
                value.setCategoryName(category.getCategoryName());
                value.setCategoryNameFetchTime(category.getFetchTimeStamp());
                return new KeyValue<>(key, value);
            });
    }
}
