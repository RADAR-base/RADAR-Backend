package org.radarcns.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by Francesco Nobilia on 26/09/2016.
 */
public class RadarConfig {

    //Properties from file config.properties
    private Properties prop;

    //Enumerate all possible topics
    public enum TopicGroup {
        in, out, all_in, mongo_sink
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
        String port = prop.getProperty("kafka.port");
        String host = prop.getProperty("kafka.listener");

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
        String port = prop.getProperty("zookeeper.port");
        String host = prop.getProperty("zookeeper.listener");

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
        String port = prop.getProperty("schemaregistry.port");
        String host = prop.getProperty("schemaregistry.url");

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
    public String getTopic(TopicGroup topic) throws NullPointerException {
        String param = null;

        switch (topic){
            case in:
                param = "topic.in";
                break;
            case out:
                param = "topic.out";
                break;
        }

        String topicName = prop.getProperty(param);

        if(topicName == null){
            throw new NullPointerException("AvroTopic is null");
        }

        return topicName;
    }

    /**
     * @param topic states which list of topics you want to retrieve
     * @return List containing topic names
     * @throws NullPointerException either the property has not been set up or the type does not exist
     */
    public List<String> getTopicList(TopicGroup topic) throws NullPointerException {
        String param = null;

        switch (topic){
            case in:
                param = "topic.in";
                break;
            case out:
                param = "topic.out";
                break;
            case all_in:
                param = "topic.list.in";
                break;
            case mongo_sink:
                param = "mongo.db.sink.topic.list";
                break;
        }

        String value = prop.getProperty(param);

        if(value == null){
            throw new NullPointerException("AvroTopic list is null");
        }

        return Arrays.asList(value.split(","));
    }

    /**
     * @return client id related to the application
     * @throws NullPointerException the property has not been set up
     */
    public String getClientID() throws NullPointerException {
        String value = prop.getProperty("client.id");

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
        String value = prop.getProperty("group.id");

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
        String value = prop.getProperty("auto.commit.interval.ms");

        if(value == null){
            throw new NullPointerException("Auto.commit.interval.ms is null");
        }

        return value;
    }

    /**
     * @return kafka consumer session timeout in milliseconds
     * @throws NullPointerException the property has not been set up
     */
    public String getSessionTimeout() throws NullPointerException {
        String value = prop.getProperty("session.timeout.ms");

        if(value == null){
            throw new NullPointerException("Session.timeout.ms is null");
        }

        return value;
    }

    /**
     * @return the number of thread for MongoDB Sink
     * @throws NullPointerException the property has not been set up
     */
    public int getMongoDBThreads() throws NullPointerException {
        String value = prop.getProperty("mongo.db.num.threads");

        if(value == null){
            throw new NullPointerException("No thread has been specified for the MongoDB Sink");
        }

        return Integer.valueOf(value);
    }

    /**
     * @return the MongoDb user
     * @throws NullPointerException the property has not been set up
     */
    public String getMongoDbUsr() throws NullPointerException {
        String value = prop.getProperty("mongo.db.usr");

        if(value == null){
            throw new NullPointerException("mongo.db.usr is null");
        }

        return value;
    }

    /**
     * @return the MongoDb password
     * @throws NullPointerException the property has not been set up
     */
    public char[] getMongoDbPwd() throws NullPointerException {
        String value = prop.getProperty("mongo.db.pwd");

        if(value == null){
            throw new NullPointerException("mongo.db.pwd is null");
        }

        return value.toCharArray();
    }

    /**
     * @return the MongoDb database name
     * @throws NullPointerException the property has not been set up
     */
    public String getMongoDbDatabaseName() throws NullPointerException {
        String value = prop.getProperty("mongo.db.dbname");

        if(value == null){
            throw new NullPointerException("mongo.db.dbname is null");
        }

        return value;
    }

    /**
     * @return the MongoDb server
     * @throws NullPointerException the property has not been set up
     */
    public String getMongoDbServer() throws NullPointerException {
        String value = prop.getProperty("mongo.db.server");

        if(value == null){
            throw new NullPointerException("mongo.db.dbname is null");
        }

        return value;
    }

    /**
     * Business function
     * @return the time-window length to consider two events as part of the same session, positive
     *         and smaller than Integer.MAX_VALUE.
     * @throws NullPointerException the property has not been set up
     * @throws NumberFormatException the property is cannot be parsed as an integer.
     */
    public int getSessionTimeWindow() throws NullPointerException, NumberFormatException {
        String value = prop.getProperty("session.length.ms");

        if (value == null){
            throw new NullPointerException("session.length.ms is null");
        }

        return Integer.valueOf(value);
    }
}
