package org.radarcns.test.logic.synch;

import org.radarcns.consumer.ConsumerGroupRadar;
import org.radarcns.consumer.ConsumerRadar;
import org.radarcns.test.logic.Sessioniser;

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
