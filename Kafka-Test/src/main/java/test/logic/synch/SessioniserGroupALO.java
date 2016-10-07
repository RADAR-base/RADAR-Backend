package test.logic.synch;

import radar.consumer.ConsumerGroupRadar;
import radar.consumer.ConsumerRadar;
import test.logic.Sessioniser;

/**
 * Created by Francesco Nobilia on 06/10/2016.
 */
public class SessioniserGroupALO extends ConsumerGroupRadar {

    private Sessioniser sessioniser;

    public SessioniserGroupALO(int numThread){
        super(numThread,"GroupALO");

        this.sessioniser = new Sessioniser();
        initiWorkers();
    }

    @Override
    public ConsumerRadar createConsumer() {
        return new SessioniserALO(sessioniser);
    }

    @Override
    public void shutdown() throws InterruptedException{
        super.shutdown();
        sessioniser.shutdown();
    }
}
