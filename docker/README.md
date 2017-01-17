## Dockerised RADAR-Backend

RADAR-Backend offers number of components such as
 1. HDFS Sink Connector as the cold storage
 2. A MongoDB connector to write to hot storage
 3. Real-time streams that aggregates subscribed topics
 4. A real-time status monitor

All these images require Zookeeper, Kafka-broker, Schema registry and Rest proxy running 
#### RADAR-HDFS Connector

To build image locally:
```
$ docker build -t "radar-hdfs-con" -f Dockerfile.hdfscon .
```
To run the docker image locally: ( In addition to the aforementioned dependencies HDFS is required to be installed and running)

This image has to be extended with a volume with appropriate sink.properties
```
$ docker run -d \
  --name radar-hdfs-connector  \
  -v pathtoyoursinkproperties:/etc/kafka-connect/sink.properties \
  -e CONNECT_BOOTSTRAP_SERVERS=localhost:39092 \
  -e CONNECT_REST_PORT=28082 \
  -e CONNECT_GROUP_ID="default" \
  -e CONNECT_CONFIG_STORAGE_TOPIC="default.config" \
  -e CONNECT_OFFSET_STORAGE_TOPIC="default.offsets" \
  -e CONNECT_STATUS_STORAGE_TOPIC="default.status" \
  -e CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_INTERNAL_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_INTERNAL_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
  -e CONNECT_REST_ADVERTISED_HOST_NAME="localhost" \
    radar-hdfs-con:latest
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
$ docker build -t "radar-mongodb-con" -f Dockerfile.mongodbcon .
```
To run the docker image locally: ( In addition to the aforementioned dependencies MongoDB is required to be installed and running)

This image has to be extended with a volume with appropriate sink.properties
```
$ docker run -d \
 --name radar-mongodb-connector \
 -v pathtoyoursinkproperties:/etc/kafka-connect/sink.properties \
 -e CONNECT_BOOTSTRAP_SERVERS=localhost:39092 \
 -e CONNECT_REST_PORT=28082 \
 -e CONNECT_GROUP_ID="default" \
 -e CONNECT_CONFIG_STORAGE_TOPIC="default.config" \
 -e CONNECT_OFFSET_STORAGE_TOPIC="default.offsets" \
 -e CONNECT_STATUS_STORAGE_TOPIC="default.status" \
 -e CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
 -e CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
 -e CONNECT_INTERNAL_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
 -e CONNECT_INTERNAL_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter" \
 -e CONNECT_REST_ADVERTISED_HOST_NAME="localhost" \
   radar-mongodb-con:latest
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
$ docker build -t "backend-stream" -f Dockerfile.streams .
```
To run the docker image locally: 
```
$ docker run -d \
 --name radar-backend-stream \
 -v ./radar.yml:/usr/local/RADAR-Backend/radar.yml \
   backend-stream:latest
```

#### RADAR-Aggregator Streams

To build image locally:
```
$ docker build -t "backend-monitor" -f Dockerfile.monitor .
```
To run the docker image locally: 
```
$ docker run -d \
 --name radar-backend-monitor \
 -v ./radar.yml:/usr/local/RADAR-Backend/radar.yml \
   backend-monitor:latest
```


A `docker-compose` file that provides the whole dependent components is give [here][https://github.com/RADAR-CNS/RADAR-Backend/blob/dev/docker/docker-compose.yml]. 

Use `sudo docker-compose up -d` to create to run the RADAR-Backend in one step. 
