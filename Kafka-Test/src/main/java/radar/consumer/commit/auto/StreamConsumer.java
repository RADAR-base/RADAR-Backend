package radar.consumer.commit.auto;

import org.apache.log4j.Logger;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import radar.consumer.ConsumerRadar;

/**
 * Created by Francesco Nobilia on 04/10/2016.
 */
public abstract class StreamConsumer extends ConsumerRadar implements Runnable{

    private final static Logger log = Logger.getLogger(StreamConsumer.class);

    private KafkaStream<Object, Object> stream;

    public abstract void execute(MessageAndMetadata<Object, Object> record);

    public abstract void shutdown();

    public void setStream(KafkaStream<Object, Object> stream) {
        this.stream = stream;
    }

    @Override
    public void run() {

        if(this.stream == null){
            throw new NullPointerException("The stream must be set up");
        }

        ConsumerIterator<Object, Object> it = stream.iterator();

        while (it.hasNext()) {
            execute(it.next());
        }
    }
}
