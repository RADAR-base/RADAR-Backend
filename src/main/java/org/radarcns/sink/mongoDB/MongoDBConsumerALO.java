package org.radarcns.sink.mongoDB;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.bson.Document;
import org.radarcns.consumer.ConsumerALO;
import org.radarcns.util.Serialization;
import org.radarcns.utils.KafkaProperties;
import org.radarcns.utils.RadarConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Francesco Nobilia on 29/09/2016.
 */
public class MongoDBConsumerALO extends ConsumerALO<byte[],byte[]> {

    private final static Logger log = LoggerFactory.getLogger(MongoDBConsumerALO.class);

    private MongoDatabase database;
    private MongoCollection collection;

    //Can be instantiated only be mongoDB sink classes
    protected MongoDBConsumerALO(MongoDatabase database){
        super(RadarConfig.PlatformTopics.mongo_sink,KafkaProperties.getSelfCommitSerdeGroupConsumer());
        initCollection(database);
    }

    protected MongoDBConsumerALO(String clientID,MongoDatabase database){
        super(clientID, RadarConfig.PlatformTopics.mongo_sink, KafkaProperties.getSelfCommitSerdeGroupConsumer(clientID,null));
        initCollection(database);
    }

    private void initCollection(MongoDatabase database){
        this.database = database;
        this.collection = this.database.getCollection("monitor");
    }

    /**
     * Implement the business auto of consumer
     */
    public void process(ConsumerRecord<byte[],byte[]> record) {
        String key = new String(record.key());
        Long value = Serialization.bytesToLong(record.value());

        Document doc = new Document("user", key).append("samples", value);

        boolean success = true;

        try{
            collection.insertOne(doc);
        } catch (MongoException e){
            log.error("Failed to insert record in MongoDB", e);
            success = false;
        }

        if(success) {
            log.trace("[{} - {}] has been written in {} collection",
                    key, value, collection.getNamespace().getCollectionName());
        }
        else{
            log.error("Error on writing [{} - {}] in {} collection",
                    key, value, collection.getNamespace().getCollectionName());
        }
    }

    @Override
    public void shutdown() throws InterruptedException{
        super.shutdown();
    }

}
