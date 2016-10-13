package test.logic.auto;

import java.util.HashMap;
import java.util.Map;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import radar.consumer.commit.auto.StreamConsumer;
import radar.utils.KafkaProperties;
import radar.utils.RadarConfig;
import radar.utils.RadarUtils;
import test.logic.Sessioniser;

/**
 * Created by Francesco Nobilia on 27/09/2016.
 */
public class SessioniserStreamed extends StreamConsumer {

    private final RadarConfig config;
    private final ConsumerConnector consumer;
    private final Sessioniser sessioniser;

    public SessioniserStreamed(){
        config = new RadarConfig();

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                new ConsumerConfig(KafkaProperties.getAutoCommitConsumer(true)));
        setStream(createStream());

        sessioniser = new Sessioniser();
    }

    private KafkaStream createStream(){
        String inputTopic = config.getTopic(RadarConfig.PlatformTopics.in);

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(config.getTopic(RadarConfig.PlatformTopics.in), 1);

        return consumer.createMessageStreams(topicCountMap, RadarUtils.getAvroDecoder(), RadarUtils.getAvroDecoder()).get(inputTopic).get(0);
    }

    @Override
    public void execute(MessageAndMetadata<Object, Object> record) {
        sessioniser.execute(record);
    }

    @Override
    public void shutdown() {
        consumer.shutdown();
        sessioniser.shutdown();
    }
}
