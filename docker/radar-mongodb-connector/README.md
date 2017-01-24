# Dockerised RADAR-MongoDB-Connector

It runs the RADAR-CNS MongoDB Connector project based on Confluent Platform 3.1.1, for more details check the [repository](https://github.com/RADAR-CNS/RADAR-MongoDbConnector).

Create the docker image:
```
$ docker build -t radarcns/radar-mongodb-connector ./
```

Or pull from dockerhub:
```
$ docker pull radarcns/radar-mongodb-connector:0.1
```

## Configuration

This image has to be extended with a volume with appropriate `sink.properties`

Sample MongoDB sink.properties
```
# Kafka consumer configuration
name=radar-connector-mongodb-sink

# Kafka connector configuration
connector.class=org.radarcns.mongodb.MongoDbSinkConnector
tasks.max=1

# Topics that will be consumed
topics=topic1, topic2

# MongoDB server
mongo.host=mongo
mongo.port=27017

# MongoDB configuration
mongo.username=
mongo.password=
mongo.database=mydbase

# Collection name for putting data into the MongoDB database. The {$topic} token will be replaced
# by the Kafka topic name.
#mongo.collection.format={$topic}

# Factory class to do the actual record conversion
record.converter.class=org.radarcns.sink.mongodb.RecordConverterFactoryRadar
```

## Runtime environment variables

This container requires two environment variable:

- `KAFKA_REST_PROXY`: a valid Rest-Proxy instance
- `TOPIC_LIST`: a comma separated list containing all required topic names

Before starting the streams, it waits until all topics inside TOPIC_LIST are available. This check is performed using the /topic Rest-Proxy API, for more details click here.

Note that connector's REST_PORT must be different from the one used by Rest-Proxy.

## How to run

For a complete use case scenario, check the RADAR-CNS `docker-compose` file available [here](https://github.com/RADAR-CNS/RADAR-Docker/blob/backend-integration/dcompose-stack/radar-cp-hadoop-stack/docker-compose.yml)