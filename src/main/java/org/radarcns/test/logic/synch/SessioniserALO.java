package org.radarcns.test.logic.synch;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.radarcns.consumer.ConsumerALO;
import org.radarcns.test.logic.Sessioniser;

/**
 * Created by Francesco Nobilia on 29/09/2016.
 */
public class SessioniserALO extends ConsumerALO<Object,Object> {

    private final Sessioniser sessioniser;
    private final boolean groupExecution;

    public SessioniserALO(){
        super();
        this.sessioniser = new Sessioniser();
        this.groupExecution = false;
    }

    public SessioniserALO(Sessioniser sessioniser){
        super();
        this.sessioniser = sessioniser;
        this.groupExecution = true;
    }

    /**
     * Implement the business auto of consumer
     */
    public void process(ConsumerRecord<Object,Object> record) {
        sessioniser.execute(record);
    }

    @Override
    public void shutdown() throws InterruptedException{
        super.shutdown();

        if(!groupExecution) {
            sessioniser.shutdown();
        }
    }

}
