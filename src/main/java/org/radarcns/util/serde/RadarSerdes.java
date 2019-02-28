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

package org.radarcns.util.serde;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.WindowStore;
import org.radarcns.stream.collector.AggregateListCollector;
import org.radarcns.stream.collector.NumericAggregateCollector;
import org.radarcns.stream.phone.PhoneUsageCollector;

/**
 * Set of Serde useful for Kafka Streams
 */
public final class RadarSerdes {

    private final Serde<NumericAggregateCollector> numericCollector;
    private final Serde<AggregateListCollector> aggregateListCollector;
    private final Serde<PhoneUsageCollector> phoneUsageCollector;

    private static RadarSerdes instance;

    public static synchronized RadarSerdes getInstance(String schemaRegistryUrls) {
        if (instance == null) {
            SchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegistryUrls,
                    100);
            instance = new RadarSerdes(client);
        }
        return instance;
    }

    private RadarSerdes(SchemaRegistryClient client) {
        numericCollector = new AvroConvertibleSerde<>(NumericAggregateCollector::new, client);
        aggregateListCollector = new AvroConvertibleSerde<>(AggregateListCollector::new, client);
        phoneUsageCollector = new RadarSerde<>(PhoneUsageCollector.class).getSerde();
    }

    public Serde<NumericAggregateCollector> getNumericAggregateCollector() {
        return numericCollector;
    }

    public Serde<AggregateListCollector> getAggregateListCollector()  {
        return aggregateListCollector;
    }

    public Serde<PhoneUsageCollector> getPhoneUsageCollector() {
        return phoneUsageCollector;
    }

    public static <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> materialized(String name, Serde<V> valueSerde) {
        Materialized<K, V, WindowStore<Bytes, byte[]>> store = Materialized.as(name);
        return store.withValueSerde(valueSerde);
    }
}
