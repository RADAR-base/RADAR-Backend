# Dockerised RADAR-HDFS-Connector

It runs the Confluent HDFS Connector 3.1.1 using a custom [RecordWriterProvider](https://github.com/RADAR-CNS/RADAR-Backend/blob/dev/src/main/java/org/radarcns/sink/hdfs/AvroRecordWriterProviderRadar.java) to support RADAR-CNS Avro schemas. For more details about Confluent HDFS Connector click [here](http://docs.confluent.io/3.1.1/connect/connect-hdfs/docs/index.html).

Create the docker image:
```
$ docker build -t radarcns/radar-hdfs-connector ./
```

Or pull from dockerhub:
```
$ docker pull radarcns/radar-hdfs-connector:0.1
```

## Configuration

This image has to be extended with a volume with appropriate `sink.properties`

Sample HDFS sink.properties
```
name=radar-hdfs-sink-android-15000
connector.class=io.confluent.connect.hdfs.HdfsSinkConnector
tasks.max=4
topics=topic1, topic2, ...
flush.size=15000
hdfs.url=hdfs://namenode:8020
format.class=org.radarcns.sink.hdfs.AvroFormatRadar
topics.dir=topicAndroidNew
```

## Runtime environment variables

This container requires two environment variable:

- `KAFKA_REST_PROXY`: a valid Rest-Proxy instance
- `TOPIC_LIST`: a comma separated list containing all required topic names

Before starting the streams, it waits until all topics inside TOPIC_LIST are available. This check is performed using the /topic Rest-Proxy API, for more details click here.

Note that connector's REST_PORT must be different from the one used by Rest-Proxy.

## How to run

For a complete use case scenario, check the RADAR-CNS `docker-compose` file available [here](https://github.com/RADAR-CNS/RADAR-Docker/blob/backend-integration/dcompose-stack/radar-cp-hadoop-stack/docker-compose.yml)