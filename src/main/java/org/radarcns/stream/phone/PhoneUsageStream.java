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

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.radarcns.config.KafkaProperty;
import org.radarcns.key.MeasurementKey;
import org.radarcns.phone.PhoneUsageEvent;
import org.radarcns.stream.StreamWorker;
import org.radarcns.stream.StreamMaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;

public class PhoneUsageStream extends StreamWorker<MeasurementKey, PhoneUsageEvent> {
    private static final Logger log = LoggerFactory.getLogger(PhoneUsageStream.class);
    //    private final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    private static final int CACHE_TIMEOUT = 24 * 3600;
    private static final int MAX_CACHE_SIZE = 1_000_000;

    private final PlayStoreLookup playStoreLookup;

    public PhoneUsageStream(String clientId, int numThread, StreamMaster master,
            KafkaProperty kafkaProperties) {
        super(PhoneStreams.getInstance().getUsageStream(), clientId,
                numThread, master, kafkaProperties, log);
        playStoreLookup =  new PlayStoreLookup(CACHE_TIMEOUT, MAX_CACHE_SIZE);
    }

    @Override
    protected void setStream(@Nonnull KStream<MeasurementKey, PhoneUsageEvent> kstream)
            throws IOException {
        kstream
            .map((key, value) -> {
                String packageName = value.getPackageName().toString();
                PlayStoreLookup.AppCategory category = playStoreLookup.lookupCategory(packageName);
                value.setCategoryName(category.getCategoryName());
                value.setCategoryNameFetchTime(category.getFetchTimeStamp());
                return new KeyValue<>(key, value);
            })
            .to(getStreamDefinition().getOutputTopic().getName());
    }

}
