package org.radarcns.test.logic.auto;

import org.apache.log4j.Logger;

import kafka.message.MessageAndMetadata;
import org.radarcns.consumer.commit.auto.StreamConsumer;
import org.radarcns.test.logic.Sessioniser;
import org.radarcns.test.producer.SimpleProducer;

/**
 * Created by Francesco Nobilia on 04/10/2016.
 */
public class SessioniserGroupWorker extends StreamConsumer {

    private final static Logger log = Logger.getLogger(SessioniserGroupWorker.class);

    private final Sessioniser sessioniser;
    private final SimpleProducer producer;

    public SessioniserGroupWorker(Sessioniser sessioniser) {
        this.sessioniser = sessioniser;
        this.producer = new SimpleProducer();
    }

    @Override
    public void execute(MessageAndMetadata<Object, Object> record) {
        sessioniser.execute(record,producer);
    }

    @Override
    public void shutdown() {
        producer.shutdown();
        //In this case sessioniser must not be closed since it is shared between all worker threads
    }

}
