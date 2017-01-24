## Dockerised RADAR-Backend

RADAR-Backend offers number of components such as
 1. HDFS Sink Connector as the cold storage
 2. A MongoDB connector to write to hot storage
 3. Real-time streams that aggregates subscribed topics
 4. A real-time status monitor

All these images require Zookeeper, Kafka-broker, Schema registry and Rest-Proxy running. Two environment variables have to be set up for all of them:

- `KAFKA_REST_PROXY`: a valid Rest-Proxy instance
- `TOPIC_LIST`: a comma separated list containing all required topic names

Before starting a RADAR-CNS component, each container waits until all topics inside `TOPIC_LIST` are available. The check is performed using the `/topic` Rest-Proxy API, for more details click [here](http://docs.confluent.io/3.1.1/kafka-rest/docs/api.html#topics). Note that connector's `REST_PORT` must be different from the one used by Rest-Proxy.

A `docker-compose` file providing all required configuration is available [here](https://github.com/RADAR-CNS/RADAR-Docker/blob/backend-integration/dcompose-stack/radar-cp-hadoop-stack/docker-compose.yml). 
