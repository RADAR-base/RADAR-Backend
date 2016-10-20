package test.logic.synch;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import radar.consumer.commit.synch.ConsumerAMO;
import test.logic.Sessioniser;

/**
 * Created by Francesco Nobilia on 29/09/2016.
 */
public class SessioniserAMO extends ConsumerAMO<Object,Object> {

    private final Sessioniser sessioniser;
    private final boolean groupExecution;

    public SessioniserAMO(){
        super();
        this.sessioniser = new Sessioniser();
        this.groupExecution = false;
    }

    public SessioniserAMO(Sessioniser sessioniser){
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
