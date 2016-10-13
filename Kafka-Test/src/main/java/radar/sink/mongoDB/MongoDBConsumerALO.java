package radar.sink.mongoDB;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.bson.Document;

import radar.consumer.commit.synch.ConsumerALO;
import radar.utils.KafkaProperties;
import radar.utils.RadarConfig;

/**
 * Created by Francesco Nobilia on 29/09/2016.
 */
public class MongoDBConsumerALO extends ConsumerALO<byte[],byte[]> {

    private final static Logger log = Logger.getLogger(MongoDBConsumerALO.class);

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
        Long value = LazyBinaryUtils.byteArrayToLong(record.value(),0);

        Document doc = new Document("user", key).append("samples", value);

        boolean success = true;

        try{
            collection.insertOne(doc);
        }
        catch (MongoWriteException e){
            log.error(e);
            success = false;
        }
        catch (MongoWriteConcernException e){
            log.error(e);
            success = false;
        }
        catch (MongoException e){
            log.error(e);
            success = false;
        }

        if(success) {
            log.trace("["+key.toString() + " - " + value.toString()+"] has been written in "+
                    collection.getNamespace().getCollectionName()+" collection");
        }
        else{
            log.error("Error on writing ["+key.toString() + " - " + value.toString()+"] in "+
                    collection.getNamespace().getCollectionName()+" collection");
        }
    }

    @Override
    public void shutdown() throws InterruptedException{
        super.shutdown();
    }

}
