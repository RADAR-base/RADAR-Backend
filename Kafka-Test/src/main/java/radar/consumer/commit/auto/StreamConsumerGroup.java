package radar.consumer.commit.auto;

import org.apache.log4j.Logger;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import radar.utils.KafkaProperties;
import radar.utils.RadarConfig;
import radar.utils.RadarUtils;

/**
 * Created by Francesco Nobilia on 04/10/2016.
 */
public class StreamConsumerGroup implements Runnable{

    private final static Logger log = Logger.getLogger(StreamConsumerGroup.class);

    private final String topic;
    private final Integer numThreads;
    private final ConsumerConnector consumer;
    private ExecutorService executor;
    private final List<StreamConsumer> workers;

    public StreamConsumerGroup(int numThreads, List<StreamConsumer> consumers){
        RadarConfig config = new RadarConfig();
        topic = config.getTopic(RadarConfig.PlatformTopics.in);

        this.numThreads = numThreads;
        this.workers = consumers;

        if(numThreads != consumers.size()){
            throw new InvalidParameterException("The number of threads and workers must be the same.");
        }

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                new ConsumerConfig(KafkaProperties.getStandardConsumer(true)));
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, numThreads);

        Map<String, List<KafkaStream<Object, Object>>> consumerMap =
                consumer.createMessageStreams(topicCountMap, RadarUtils.getAvroDecoder(), RadarUtils.getAvroDecoder());
        List<KafkaStream<Object, Object>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(numThreads);

        for(int i=0; i<numThreads; i++){
            StreamConsumer worker = workers.get(i);
            worker.setStream(streams.get(i));

            executor.submit(worker);
        }
    }

    public void shutdown() {
        if(consumer != null){
            consumer.shutdown();
        }

        if(executor != null){
            executor.shutdown();
        }

        if(workers != null){
            workers.forEach(consumer -> consumer.shutdown());
        }

        try{
            if(!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)){
                log.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        }catch(InterruptedException e){
            log.error("Interrupted during shutdown, exiting uncleanly");
        }
    }
}
