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

package org.radarcns.topic;

import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;

import java.util.List;

/** KafkaTopic with schema */
public class AvroTopic<K, V> extends KafkaTopic {
    private final Schema valueSchema;
    private final Schema keySchema;
    private final Schema.Type[] valueFieldTypes;
    private final Class<V> valueClass;
    private final Class<K> keyClass;

    public AvroTopic(@Nonnull String name, @Nonnull Schema keySchema, @Nonnull Schema valueSchema,
            @Nonnull Class<K> keyClass, @Nonnull Class<V> valueClass) {
        super(name);
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.valueClass = valueClass;
        this.keyClass = keyClass;

        List<Schema.Field> fields = valueSchema.getFields();
        this.valueFieldTypes = new Schema.Type[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            valueFieldTypes[i] = fields.get(i).schema().getType();
        }
    }

    public Schema getKeySchema() {
        return keySchema;
    }

    public Schema getValueSchema() {
        return valueSchema;
    }

    public Class<V> getValueClass() {
        return valueClass;
    }

    public Class<K> getKeyClass() {
        return keyClass;
    }

    public Schema.Type[] getValueFieldTypes() {
        return valueFieldTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass() || !super.equals(o)) {
            return false;
        }

        AvroTopic topic = (AvroTopic) o;

        return keyClass == topic.getKeyClass()
                && valueClass == topic.getValueClass();
    }

    @Override
    public int hashCode() {
        return keyClass.hashCode() + 31 * valueClass.hashCode();
    }
}
