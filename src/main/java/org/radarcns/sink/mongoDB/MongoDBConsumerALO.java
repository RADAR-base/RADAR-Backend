package org.radarcns.sink.mongoDB;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.radarcns.consumer.ConsumerALO;
import org.radarcns.util.KafkaProperties;
import org.radarcns.util.RadarConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Francesco Nobilia on 29/09/2016.
 */
public class MongoDBConsumerALO extends ConsumerALO<Object,Object> {
    private final static Logger log = LoggerFactory.getLogger(MongoDBConsumerALO.class);

    private final MongoDatabase database;
    private final MongoCollection collection;

    protected MongoDBConsumerALO(String clientID,MongoDatabase database){
        super(clientID, RadarConfig.TopicGroup.mongo_sink, KafkaProperties.getAutoCommitConsumer(true,clientID));
        this.database = database;
        this.collection = this.database.getCollection("monitor");
    }

    /**
     * Implement the business auto of consumer
     */
    public void process(ConsumerRecord<Object,Object> record) {

        log.trace("{}",record.toString());

        /*WindowedKey key = (WindowedKey) SpecificData.get().deepCopy(WindowedKey.SCHEMA$, record.key());
        Statistic statistic = (Statistic) SpecificData.get().deepCopy(Statistic.SCHEMA$, record.value());

        String mongoId = key.getUserID()+"-"+key.getSourceID()+"-"+key.getStart()+"-"+key.getEnd();

        LinkedList<Document> quartile = new LinkedList<>();
        quartile.addLast(new Document("25", new BsonDouble(statistic.getQuartile().get(0))));
        quartile.addLast(new Document("50", new BsonDouble(statistic.getQuartile().get(1))));
        quartile.addLast(new Document("75", new BsonDouble(statistic.getQuartile().get(2))));



        Document doc = new Document("_id", mongoId)
                                //.append("user", new BsonInt64(key.getUserID()))
                                //.append("source", new BsonInt64(key.getSourceID()))
                                .append("user", key.getUserID())
                                .append("source", key.getSourceID())
                                .append("min", new BsonDouble(statistic.getMin()))
                                .append("max", new BsonDouble(statistic.getMax()))
                                .append("sum", new BsonDouble(statistic.getSum()))
                                .append("count", new BsonDouble(statistic.getCount()))
                                .append("avg", new BsonDouble(statistic.getAvg()))
                                .append("quartile", quartile)
                                .append("iqr", new BsonDouble(statistic.getIqr()))
                                .append("start", new BsonDateTime(key.getStart()))
                                .append("end", new BsonDateTime(key.getEnd()));

        try{
            collection.replaceOne(eq("_id", mongoId),doc,(new UpdateOptions()).upsert(true));
            log.trace("[{} - {}] has been written in {} collection",
                    key, statistic, collection.getNamespace().getCollectionName().toString());
        } catch (MongoException e){
            log.error("Failed to insert record in MongoDB", e);
            log.error("Error on writing [{} - {}] in {} collection",
                    key, statistic, collection.getNamespace().getCollectionName().toString());
        }*/
    }

    @Override
    protected void handleSerializationError(SerializationException e) {
        log.error("Cannot deserialize", e);
    }
}
