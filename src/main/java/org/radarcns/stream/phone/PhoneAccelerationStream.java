package org.radarcns.stream.phone;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.radarcns.aggregator.DoubleArrayAggregator;
import org.radarcns.config.KafkaProperty;
import org.radarcns.key.MeasurementKey;
import org.radarcns.key.WindowedKey;
import org.radarcns.phone.PhoneAcceleration;
import org.radarcns.stream.StreamWorker;
import org.radarcns.stream.collector.DoubleArrayCollector;
import org.radarcns.util.RadarSingletonFactory;
import org.radarcns.util.RadarUtilities;
import org.radarcns.util.serde.RadarSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static org.radarcns.util.Serialization.floatToDouble;

public class PhoneAccelerationStream extends StreamWorker<MeasurementKey, PhoneAcceleration> {
    private static final Logger logger = LoggerFactory.getLogger(PhoneAccelerationStream.class);
    private final RadarUtilities utilities = RadarSingletonFactory.getRadarUtilities();

    public PhoneAccelerationStream(String phoneAccelerationStream, int priority, PhoneStreamMaster phoneStreamMaster, KafkaProperty kafkaProperty) {
        super(PhoneStreams.getInstance().getAccelerationStream(), phoneAccelerationStream,
                priority, phoneStreamMaster,
                kafkaProperty, logger);
    }

    @Override
    protected KStream<WindowedKey, DoubleArrayAggregator> defineStream(
            @Nonnull KStream<MeasurementKey, PhoneAcceleration> kstream) {
        return kstream.groupByKey()
                .aggregate(
                        DoubleArrayCollector::new,
                        (k, v, valueCollector) -> valueCollector.add(new double[] {
                            floatToDouble(v.getX()),
                            floatToDouble(v.getY()),
                            floatToDouble(v.getZ())
                        }),
                        TimeWindows.of(10 * 1000L),
                        RadarSerdes.getInstance().getDoubleArrayCollector(),
                        getStreamDefinition().getStateStoreName())
                .toStream()
                .map(utilities::collectorToAvro);
    }
}
