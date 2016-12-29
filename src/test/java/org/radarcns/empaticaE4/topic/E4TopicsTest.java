package org.radarcns.empaticaE4.topic;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by nivethika on 21-12-16.
 */
public class E4TopicsTest {
    private E4Topics e4Topics ;

    @Before
    public void setUp() {
        this.e4Topics = E4Topics.getInstance();
    }

    @Test
    public void getSensorTopics() {
        assertEquals(this.e4Topics.getSensorTopics(), E4SensorTopics.getInstance());
    }

    @Test
    public void getInternalTopics() {
        assertEquals(this.e4Topics.getInternalTopics(), E4InternalTopics.getInstance());
    }

    @Test
    public void getTopicNames() {
        assertEquals(15, this.e4Topics.getTopicNames().size()); // sort removes the redundant
    }
}
