package org.radarcns.sink.mongodb;

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
        List<RecordConverter> recordConverters = super.genericConverters();
        recordConverters.add(new BatteryLevelRecordConverter());
        return recordConverters;
    }

}
