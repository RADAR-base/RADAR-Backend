package org.radarcns.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by Francesco Nobilia on 26/09/2016.
 */
public class RadarConfig {

    private final static Logger log = LoggerFactory.getLogger(RadarConfig.class);

    //PropertiesRadar from file config.properties
    private Properties prop;

    //Enumerate all possible topics
    public enum TopicGroup {
        in("topic.in"), out("topic.out"), all_in("topic.list.in"),
        mongo_sink("mongo.db.sink.topic.list");

        private final String param;

        TopicGroup(String param) {
            this.param = param;
        }

        public String getParam() {
            return param;
        }
    }

    public RadarConfig() {
        String configFile = "configuration/config.properties";
        prop = new Properties();

        try (InputStream in = getClass().getClassLoader().getResourceAsStream(configFile)){
            // load a properties file
            prop.load(in);
        } catch (IOException io) {
            io.printStackTrace();
        }
    }

    /**
     * @return the URL for Kafka brokers
     * @throws NullPointerException if the property has not been set up
     */
    public String getKafkaListener() throws NullPointerException {
        return getHostAndPort("kafka.listener", "kafka.port");
    }

    /**
     * @return the URL for the ZooKeeper instance
     * @throws NullPointerException if the property has not been set up
     */
    public String getZooKeeperURL() throws NullPointerException {
        return getHostAndPort("zookeeper.listener", "zookeeper.port");
    }

    /**
     * @return the URL for the Schema Registry
     * @throws NullPointerException if the property has not been set up
     */
    public String getSchemaRegistryURL() throws NullPointerException {
        return getHostAndPort("schemaregistry.url", "schemaregistry.port");
    }

    /**
     * @param topic states which topic you want to retrieve
     * @return the topic name
     * @throws NullPointerException either the property has not been set up or the type does not
     *                              exist
     */
    public String getTopic(TopicGroup topic) throws NullPointerException {
        switch (topic){
            case in:
            case out:
                return getOrThrow(topic.getParam());
            default:
                throw new NullPointerException("topic " + topic + " is not a single topic");
        }
    }

    /**
     * @param topic states which list of topics you want to retrieve
     * @return List containing topic names
     * @throws NullPointerException either the property has not been set up or the type does not
     *                              exist
     */
    public List<String> getTopicList(TopicGroup topic) throws NullPointerException {
        String[] topics = getOrThrow(topic.getParam()).split(",");
        return Arrays.asList(topics);
    }

    /**
     * @return client id related to the application
     * @throws NullPointerException the property has not been set up
     */
    public String getClientID() throws NullPointerException {
        return getOrThrow("client.id");
    }

    /**
     * @return group id related to the operation
     * @throws NullPointerException the property has not been set up
     */
    public String getGroupID() throws NullPointerException {
        return getOrThrow("group.id");
    }

    /**
     * @return interval in milliseconds between two consecutive commits
     * @throws NullPointerException the property has not been set up
     */
    public String getAutoCommitInterval() throws NullPointerException {
        return getOrThrow("auto.commit.interval.ms");
    }

    /**
     * @return kafka consumer session timeout in milliseconds
     * @throws NullPointerException the property has not been set up
     */
    public String getSessionTimeout() throws NullPointerException {
        return getOrThrow("session.timeout.ms");
    }

    /**
     * @return the number of thread for MongoDB Sink
     * @throws NullPointerException the property has not been set up
     */
    public int getMongoDBThreads() throws NumberFormatException, NullPointerException {
        return Integer.valueOf(getOrThrow("mongo.db.num.threads"));
    }

    /**
     * @return the MongoDb user
     * @throws NullPointerException the property has not been set up
     */
    public String getMongoDbUsr() throws NullPointerException {
        return getOrThrow("mongo.db.usr");
    }

    /**
     * @return the MongoDb password
     * @throws NullPointerException the property has not been set up
     */
    public char[] getMongoDbPwd() throws NullPointerException {
        return getOrThrow("mongo.db.pwd").toCharArray();
    }

    /**
     * @return the MongoDb database name
     * @throws NullPointerException the property has not been set up
     */
    public String getMongoDbDatabaseName() throws NullPointerException {
        return getOrThrow("mongo.db.dbname");
    }

    /**
     * @return the MongoDb server
     * @throws NullPointerException the property has not been set up
     */
    public String getMongoDbServer() throws NullPointerException {
        return getOrThrow("mongo.db.server");
    }

    /**
     * Business function
     * @return the time-window length to consider two events as part of the same session, positive
     *         and smaller than Integer.MAX_VALUE.
     * @throws NullPointerException the property has not been set up
     * @throws NumberFormatException the property is cannot be parsed as an integer.
     */
    public int getSessionTimeWindow() throws NullPointerException, NumberFormatException {
        return Integer.valueOf(getOrThrow("session.length.ms"));
    }

    private String getOrThrow(String property) throws NullPointerException {
        String value = prop.getProperty(property);

        if (value == null){
            throw new NullPointerException(property + " is null");
        }

        return value;
    }

    private String getHostAndPort(String hostProperty, String portProperty) {
        String host = getOrThrow(hostProperty);
        String port = prop.getProperty(portProperty);

        if (port != null) {
            return host + ":" + port;
        } else {
            return host;
        }
    }
}
