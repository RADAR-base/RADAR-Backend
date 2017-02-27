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
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.radarcns.aggregator.DoubleArrayAggregator;
import org.radarcns.key.WindowedKey;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.util.Utility;

public class AggregatedAccelerationRecordConverter implements RecordConverter {
    @Override
    public Collection<String> supportedSchemaNames() {
        return Collections.singleton(WindowedKey.class.getCanonicalName() + "-"
                + DoubleArrayAggregator.class.getCanonicalName());
    }

    @Override
    public Document convert(SinkRecord record) {
        Struct key = (Struct) record.key();
        Struct value = (Struct) record.value();

        return new Document("_id", Utility.intervalKeyToMongoKey(key))
                .append("user", key.getString("userID"))
                .append("source", key.getString("sourceID"))
                .append("min", accCompToDoc(value.getArray("min")))
                .append("max", accCompToDoc(value.getArray("max")))
                .append("sum", accCompToDoc(value.getArray("sum")))
                .append("count", accCompToDoc(value.getArray("count")))
                .append("avg", accCompToDoc(value.getArray("avg")))
                .append("quartile", accQuartileToDoc(value.getArray("quartile")))
                .append("iqr", accCompToDoc(value.getArray("iqr")))
                .append("start", new BsonDateTime(key.getInt64("start")))
                .append("end", new BsonDateTime(key.getInt64("end")));
    }

    private static Document accCompToDoc(List<Double> component) {
        return new Document("x", component.get(0))
                .append("y", component.get(1))
                .append("z", component.get(2));
    }

    private static Document accQuartileToDoc(List<List<Double>> list) {
        return new Document("x", Utility.extractQuartile(list.get(0)))
                .append("y", Utility.extractQuartile(list.get(1)))
                .append("z", Utility.extractQuartile(list.get(2)));
    }
}
