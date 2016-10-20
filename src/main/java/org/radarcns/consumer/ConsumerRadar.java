package org.radarcns.consumer;

/**
 * Created by Francesco Nobilia on 04/10/2016.
 */
public abstract class ConsumerRadar implements Runnable{

    public abstract void shutdown() throws InterruptedException;
}
