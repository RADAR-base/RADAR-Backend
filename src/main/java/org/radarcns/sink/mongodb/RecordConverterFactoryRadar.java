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

package org.radarcns.sink.mongodb;

import java.util.ArrayList;
import java.util.List;
import org.radarcns.serialization.RecordConverter;
import org.radarcns.serialization.RecordConverterFactory;

/**
 * Extended RecordConverterFactory to allow customized RecordConverter class that are needed
 */
public class RecordConverterFactoryRadar extends RecordConverterFactory {

    /**
     * Overrides genericConverter to append custom RecordConverter class to RecordConverterFactory
     *
     * @return list of RecordConverters available
     */
    protected List<RecordConverter> genericConverters() {
        List<RecordConverter> recordConverters = new ArrayList<RecordConverter>();
        recordConverters.addAll(super.genericConverters());
        recordConverters.add(new AggregatedAccelerationRecordConverter());
        recordConverters.add(new DoubleAggregatedRecordConverter());
        recordConverters.add(new BatteryLevelRecordConverter());
        recordConverters.add(new CountsStatusRecordConverter());
        recordConverters.add(new ServerStatusRecordConverter());
        recordConverters.add(new UptimeStatusRecordConverter());
        return recordConverters;
    }

}
