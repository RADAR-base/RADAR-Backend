package radar.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nonnull;

/**
 * Created by Francesco Nobilia on 26/09/2016.
 */
public class RadarConfig {

    //Properties from file config.properties
    private Properties prop;

    //Enumerate all possible topics
    public enum PlatformTopics {
        in, out, all_in
    }

    public RadarConfig() {

        prop = new Properties();

        InputStream input = null;

        try {

            File file = new File(getClass().getClassLoader().getResource("configuration/config.properties").getFile());
            input = new FileInputStream(file);

            // load a properties file
            prop.load(input);

        } catch (IOException io) {
            io.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * @return the URL for Kafka brokers
     * @throws NullPointerException if the property has not been set up
     */
    public String getKafkaListener() throws NullPointerException {
        String port = prop.getProperty("kafka_port");
        String host = prop.getProperty("kafka_listener");

        if(host == null){
            throw new NullPointerException("Kafka listener path is missing");
        }

        if(port == null){
            return host;
        }

        return host+":"+port;
    }

    /**
     * @return the URL for the ZooKeeper instance
     * @throws NullPointerException if the property has not been set up
     */
    public String getZooKeeperURL() throws NullPointerException {
        String port = prop.getProperty("zookeeper_port");
        String host = prop.getProperty("zookeeper_listener");

        if(host == null){
            throw new NullPointerException("ZooKeeper URL is null");
        }

        if(port == null){
            return host;
        }

        return host+":"+port;
    }

    /**
     * @return the URL for the Schema Registry
     * @throws NullPointerException if the property has not been set up
     */
    public String getSchemaRegistryURL() throws NullPointerException {
        String port = prop.getProperty("schemaregistry_port");
        String host = prop.getProperty("schemaregistry_url");

        if(host == null){
            throw new NullPointerException("SchemaRegistry URL is null");
        }

        if(port == null){
            return host;
        }

        return host+":"+port;
    }

    /**
     * @param topic states which topic you want to retrieve
     * @return the topic name
     * @throws NullPointerException either the property has not been set up or the type does not exist
     */
    public String getTopic(@Nonnull PlatformTopics topic) throws NullPointerException {
        String param = null;

        switch (topic){
            case in:
                param = "topic_in";
                break;
            case out:
                param = "topic_out";
                break;
        }

        String topicName = prop.getProperty(param);

        if(topicName == null){
            throw new NullPointerException("Topic is null");
        }

        return topicName;
    }

    /**
     * @param topic states which list of topics you want to retrieve
     * @return List containing topic names
     * @throws NullPointerException either the property has not been set up or the type does not exist
     */
    public List<String> getTopicList(@Nonnull PlatformTopics topic) throws NullPointerException {
        String param = null;

        switch (topic){
            case in:
                param = "topic_in";
                break;
            case out:
                param = "topic_out";
                break;
            case all_in:
                param = "topic_list_in";
                break;
        }

        String value = prop.getProperty(param);

        if(value == null){
            throw new NullPointerException("Topic list is null");
        }

        return Arrays.asList(value.split(","));
    }

    /**
     * @return client id related to the application
     * @throws NullPointerException the property has not been set up
     */
    public String getClientID() throws NullPointerException {
        String value = prop.getProperty("client_id");

        if(value == null){
            throw new NullPointerException("Client.id is null");
        }

        return value;
    }

    /**
     * @return group id related to the operation
     * @throws NullPointerException the property has not been set up
     */
    public String getGroupID() throws NullPointerException {
        String value = prop.getProperty("group_id");

        if(value == null){
            throw new NullPointerException("Group.id is null");
        }

        return value;
    }

    /**
     * @return interval in milliseconds between two consecutive commits
     * @throws NullPointerException the property has not been set up
     */
    public String getAutoCommitInterval() throws NullPointerException {
        String value = prop.getProperty("auto_commit_interval_ms");

        if(value == null){
            throw new NullPointerException("Group.id is null");
        }

        return value;
    }

    /**
     * @return kafka consumer session timeout in milliseconds
     * @throws NullPointerException the property has not been set up
     */
    public String getSessionTimeout() throws NullPointerException {
        String value = prop.getProperty("session_timeout_ms");

        if(value == null){
            throw new NullPointerException("Group.id is null");
        }

        return value;
    }

    /**
     * Business function
     * @return the time-window length to consider two events as part of the same session
     * @throws NullPointerException the property has not been set up
     */
    public Long getSessionTimeWindow() throws NullPointerException {
        String value = prop.getProperty("session_length_ms");

        if(value == null){
            throw new NullPointerException("SchemaRegistry URL is null");
        }

        return Long.valueOf(value);
    }
}
