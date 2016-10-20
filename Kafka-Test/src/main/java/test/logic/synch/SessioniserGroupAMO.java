package test.logic.synch;

import radar.consumer.ConsumerGroupRadar;
import radar.consumer.ConsumerRadar;
import test.logic.Sessioniser;

/**
 * Created by Francesco Nobilia on 06/10/2016.
 */
public class SessioniserGroupAMO extends ConsumerGroupRadar {

    private Sessioniser sessioniser;

    public SessioniserGroupAMO(int numThread){
        super(numThread,"GroupAMO");

        this.sessioniser = new Sessioniser();
        initiWorkers();
    }

    @Override
    public ConsumerRadar createConsumer() {
        return new SessioniserAMO(sessioniser);
    }

    @Override
    public void shutdown() throws InterruptedException{
        super.shutdown();
        sessioniser.shutdown();
    }
}
