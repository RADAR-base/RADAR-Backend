package org.radarcns.empaticaE4.topic;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.radarcns.topic.AvroTopic;
import org.radarcns.topic.InternalTopic;

import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Created by nivethika on 21-12-16.
 */
public class E4InternalTopicsTest {
    private E4InternalTopics internalTopics;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        this.internalTopics = E4InternalTopics.getInstance();
    }
    @Test
    public void getTopic() {
        InternalTopic topic = this.internalTopics.getTopic("android_empatica_e4_heartrate");
        assertEquals("android_empatica_e4_inter_beat_interval", topic.getInputTopic());
        assertEquals("android_empatica_e4_heartrate_output", topic.getOutputTopic());
        assertEquals("android_empatica_e4_heartrate_store", topic.getStateStoreName());
        assertEquals(2, topic.getAllTopicNames().size());
    }

    @Test
    public void getInvalidTopic() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("Topic something unknown");
        InternalTopic topic = this.internalTopics.getTopic("something");
    }

    @Test
    public void getTopicNames() {
        Set<String> topics = this.internalTopics.getTopicNames();
        assertEquals(2, topics.size());
        assertEquals(this.internalTopics.getHeartRateTopic().getAllTopicNames().toArray()[1],topics.toArray()[0]);
        assertEquals(this.internalTopics.getHeartRateTopic().getAllTopicNames().toArray()[0],topics.toArray()[1]);
    }
}
