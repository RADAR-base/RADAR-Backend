/*
 *  Copyright 2016 Kings College London and The Hyve
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

package org.radarcns.sink.mongodb;

import java.util.Collection;
import java.util.Collections;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDateTime;
import org.bson.BsonDouble;
import org.bson.Document;
import org.radarcns.aggregator.DoubleAggregator;
import org.radarcns.key.WindowedKey;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.util.Utility;

public class DoubleAggregatedRecordConverter implements RecordConverter {
    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton(WindowedKey.class.getCanonicalName() + "-"
                + DoubleAggregator.class.getCanonicalName());
    }

    @Override
    public Document convert(SinkRecord record) {
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();

        return new Document("_id", Utility.intervalKeyToMongoKey(key))
                .append("user", key.getString("userID"))
                .append("source", key.getString("sourceID"))
                .append("min", new BsonDouble(value.getFloat64("min")))
                .append("max", new BsonDouble(value.getFloat64("max")))
                .append("sum", new BsonDouble(value.getFloat64("sum")))
                .append("count", new BsonDouble(value.getFloat64("count")))
                .append("avg", new BsonDouble(value.getFloat64("avg")))
                .append("quartile", Utility.extractQuartile(value.getArray("quartile")))
                .append("iqr", new BsonDouble(value.getFloat64("iqr")))
                .append("start", new BsonDateTime(key.getInt64("start")))
                .append("end", new BsonDateTime(key.getInt64("end")));
    }
}
