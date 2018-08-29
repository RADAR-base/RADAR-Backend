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
import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.RadarPropertyHandler.Priority;
import org.radarcns.kafka.ObservationKey;
import org.radarcns.passive.phone.PhoneUsageEvent;
import org.radarcns.stream.SensorStreamWorker;
import org.radarcns.stream.StreamDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoneUsageStream extends SensorStreamWorker<ObservationKey, PhoneUsageEvent> {
    private static final Logger logger = LoggerFactory.getLogger(PhoneUsageStream.class);

    // 1 day until an item is refreshed
    private static final int CACHE_TIMEOUT = 24 * 3600;

    // Do not cache more than 1 million elements, for memory consumption reasons
    private static final int MAX_CACHE_SIZE = 1_000_000;

    private final PlayStoreLookup playStoreLookup;

    public PhoneUsageStream() {
        playStoreLookup = new PlayStoreLookup(CACHE_TIMEOUT, MAX_CACHE_SIZE);
    }

    @Override
    protected void initialize() {
        defineSensorStream("android_phone_usage_event");
        config.setDefaultPriority(Priority.LOW);
    }

    @Override
    protected KStream<ObservationKey, PhoneUsageEvent> implementStream(StreamDefinition definition,
            @Nonnull KStream<ObservationKey, PhoneUsageEvent> kstream) {
        return kstream
            .mapValues(value -> {
                String packageName = value.getPackageName();
                PlayStoreLookup.AppCategory category = playStoreLookup.lookupCategory(packageName);
                logger.info("Looked up {}: {}", packageName, category.getCategoryName());
                value.setCategoryName(category.getCategoryName());
                value.setCategoryNameFetchTime(category.getFetchTimeStamp());
                return value;
            });
    }
}
