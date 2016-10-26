# RADAR-CNS backend

A Java backend using Confluent Platform to investigate how to process streaming data.

In the `org.radarcns` package there are the abstraction classes that can be implemented for easily create Kafka Consumer and Kafka Consumers Group. Data exchange relies on Apache Avro. We manage keys and values serialised by Avro, the using schemas aveilable at /resources/avro

## Test case

This test case implements a Sessionization: a basic step to prepare website click to be analysed. It tries to detect whether two consecutive visits from the same IP address belong to the same session or not. The involved Kafka messages contain keys and values serialized in Avro.

This example demonstrates 4 important design patterns:

- Consuming records from a topic, filtering or modifying them and producing them to a new topic
- Partitioning data based on meaningful fields to allow more efficient processing later
- Consuming and producing Avro records using the Avro serializers
- Consuming messages using groups

## Quickstart 

```shell
# Start Zookeeper
$ bin/zookeeper-server-start config/zookeeper.properties

# Start Kafka
$ bin/kafka-server-start config/server.properties

# Start Schema Registry
$ bin/schema-registry-start config/schema-registry.properties

# Create page_visits topic
$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 \
  --partitions 3 --topic clicks

# Create session topic
$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 \
  --partitions 1 --topic sessionized_clicks
```

## Contributing

Code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html). If you want to contribute a feature or fix, please make a pull request

## Source

This is a re-elaborated version of this code at https://github.com/confluentinc/examples
