## Dockerised RADAR-Backend

RADAR-Backend offers number of components such as
 1. HDFS Sink Connector as the cold storage
 2. A MongoDB connector to write to hot storage
 3. Real-time streams that aggregates subscribed topics
 4. A real-time status monitor

All these images require Zookeeper, Kafka-broker, Schema registry and Rest-Proxy running. Two environment variables have to be set up for all of them:
 -`KAFKA_REST_PROXY`: a valid Rest-Proxy instance
 -`TOPIC_LIST`: a comma separated list containing all required topic names
Before starting a RADAR-CNS component, each container waits until all topics inside `TOPIC_LIST` are available. The check is performed using the `/topic` Rest-Proxy API, for more details click [here](http://docs.confluent.io/3.1.1/kafka-rest/docs/api.html#topics). Note that connector's `REST_PORT` must be different from the one used by Rest-Proxy.

#### RADAR-HDFS Connector

To build image locally:
```
docker-compose build radar-hdfs-connector
```
To run the docker image locally: ( In addition to the aforementioned dependencies HDFS is required to be installed and running)

This image has to be extended with a volume with appropriate sink.properties
```
docker-compose up radar-hdfs-connector
```

Sample HDFS sink.properties
```
name=radar-hdfs-sink-android-15000
connector.class=io.confluent.connect.hdfs.HdfsSinkConnector
tasks.max=4
topics=<topics>
flush.size=15000
hdfs.url=hdfs://namenode:8020
format.class=org.radarcns.sink.hdfs.AvroFormatRadar
topics.dir=topicAndroidNew
```

#### RADAR-MongoDB Connector

To build image locally:
```
docker-compose build radar-mongodb-connector
```
To run the docker image locally: ( In addition to the aforementioned dependencies MongoDB is required to be installed and running)

This image has to be extended with a volume with appropriate sink.properties, see the `docker-compose.yml` for their details.
```
docker-compose up radar-mongodb-connector
```

Sample MongoDB sink.properties
```
# Kafka consumer configuration
name=radar-connector-mongodb-sink

# Kafka connector configuration
connector.class=org.radarcns.mongodb.MongoDbSinkConnector
tasks.max=1

# Topics that will be consumed
topics=android_empatica_e4_battery_level,android_empatica_e4_battery_level_output

# MongoDB server
mongo.host=mongo
mongo.port=27017

# MongoDB configuration
#mongo.username=
#mongo.password=
mongo.database=mydbase

# Collection name for putting data into the MongoDB database. The {$topic} token will be replaced
# by the Kafka topic name.
#mongo.collection.format={$topic}

# Factory class to do the actual record conversion
record.converter.class=org.radarcns.sink.mongodb.RecordConverterFactoryRadar

```

#### RADAR-Aggregator Streams

To build image locally:
```
docker-compose build radar-backend-stream
```
To run the docker image locally: 
```
docker-compose up radar-backend-stream
```

Edit the `radar.yml` file to configure the streams.

#### RADAR topic monitor

To build image locally:
```
docker-compose build radar-backend-monitor
```
To run the docker image locally: 
```
docker-compose run radar-backend-monitor
```

Edit the `radar.yml` file to configure the monitors.


A `docker-compose` file that provides the whole dependent components is give [here](https://github.com/RADAR-CNS/RADAR-Backend/blob/dev/docker/docker-compose.yml). 

Use `sudo docker-compose up -d` to create to run the RADAR-Backend in one step. 
