# Dockerised RADAR-Backend-Kafka

It runs the RADAR-CNS Backend Kafka solution based on Kafka Streams 3.1.1, for more details about Kafka Streams click [here](http://docs.confluent.io/3.1.1/streams/index.html).

Create the docker image:
```
$ docker build -t radarcns/radar-backend-kafka ./
```

Or pull from dockerhub:
```
$ docker pull radarcns/radar-backend-kafka:0.1
```

## Configuration

Edit the radar.yml file to configure either the streams or the monitor.

## Runtime environment variables

This container requires two environment variable:

- `KAFKA_REST_PROXY`: a valid Rest-Proxy instance
- `TOPIC_LIST`: a comma separated list containing all required topic names

Before starting the streams, it waits until all topics inside TOPIC_LIST are available. This check is performed using the /topic Rest-Proxy API, for more details click here.

## How to run

For a complete use case scenario, check the RADAR-CNS `docker-compose` file available [here](https://github.com/RADAR-CNS/RADAR-Docker/blob/backend-integration/dcompose-stack/radar-cp-hadoop-stack/docker-compose.yml)