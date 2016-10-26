package org.radarcns.sink.mongoDB;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.bson.Document;
import org.radarcns.consumer.ConsumerALO;
import org.radarcns.util.KafkaProperties;
import org.radarcns.util.RadarConfig;
import org.radarcns.util.Serialization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Francesco Nobilia on 29/09/2016.
 */
public class MongoDBConsumerALO extends ConsumerALO<byte[],byte[]> {
    private final static Logger log = LoggerFactory.getLogger(MongoDBConsumerALO.class);

    private final MongoDatabase database;
    private final MongoCollection collection;

    protected MongoDBConsumerALO(String clientID,MongoDatabase database){
        super(clientID, RadarConfig.TopicGroup.mongo_sink, KafkaProperties.getSelfCommitSerdeGroupConsumer(clientID,null));
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
    protected void handleSerializationError(SerializationException e) {
        log.error("Cannot deserialize", e);
    }
}
