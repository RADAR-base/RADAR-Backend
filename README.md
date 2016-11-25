# RADAR-CNS back-end

A Java application based on Confluent Platform to standardise, analyse and persistent data collected by RADAR-CNS data sources. Data is consumed and produced in Apache Avro format using the schemas stored inside the RADAR-CNS [schema repository](https://github.com/RADAR-CNS/RADAR-Schemas).

Currently only the Empatica E4 is supported.

# Generalisation

The Kafka Streams concept has been generalised. [MasterAggregator]() defines the stream master while [AggregatorWorker]() represents the stream slave. The master stream creates, starts and stops a list of stream slave. Two types of slave stream have been defined:
- [SensorAggregator]() consumes and aggregates topics without performing any transformations
- [InternalAggregator]() modifies incoming data producing a new information
While the classical Kafka Consumer requires two implementations to support standalone and group executions, the stream slave definition provides both behaviours with one implementation.

While the former consumes [SensorTopic<V>](), the latter reads [InternalTopic<O>](). These topics are a specialisation of [AvroTopic<K,V>]().
- [SensorTopic<V>]() defines a set of topic used to consume and aggregate data as is
- [InternalTopic<O>]() delineates a set of topic used to consume and aggregate transformed data
[SensorTopics]() and [InternalTopics]() drive the developer to the implementation of sets of topics containing only one topic type. To improve the flexibility, these sets are then unified by [DeviceTopics](). All these set should be defined following the [Factory Method Pattern](https://en.wikipedia.org/wiki/Factory_method_pattern). It should be used for each element that has to be unique within the application (e.g. [MasterAggregator]())

[DeviceTimestampExtractor]() implements a [TimestampExtractor](http://docs.confluent.io/3.1.0/streams/javadocs/index.html) such that: given in input a generic APACHE Avro object, it extracts a field named `timeReceived`. [DeviceTimestampExtractor]() works with the entire set of sensor schemas currently available.

## Empatica E4

[E4Worker]() is the [MasterAggregator](). [E4SensorTopics]() and [E4InternalTopics]() are respectively [SensorTopics]() and [InternalTopics](). [E4Topics]() (i.e. [DeviceTopics]()) are consumed by:
- [E4Acceleration](): it aggregates data coming from accelerometer
- [E4BatteryLevel](): it aggregates battery level information
- [E4BloodVolumePulse](): it aggregates blood volume pulse data
- [E4ElectroDermalActivity](): it aggregates electrodermal activity informations
- [E4HeartRate](): starting from the inter beat interval, this aggregator computes the heart rate value  (i.e. [InternalAggregator]())
- [E4InterBeatInterval](): it aggregates inter beat interval data

## Contributing

To add additional devices, make the following steps (see the `org.radarcns.empaticaE4` package as an example):
- create [SensorTopics]() and if needed [InternalTopics]() then unify them in [DeviceTopics]()
- for each topic create either [SensorAggregator]() or [InternalAggregator]()
- define your [MasterAggregator]()

Code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html).

If you want to contribute a feature or fix, please make a pull request

# Quickstart for Empatica

## Confluent Platform

```shell
# Start Zookeeper
$ bin/zookeeper-server-start config/zookeeper.properties

# Start Kafka
$ bin/kafka-server-start config/server.properties

# Start Schema Registry
$ bin/schema-registry-start config/schema-registry.properties

# Start Schema Registry
$ bin/schema-registry-start config/schema-registry.properties

# Start Rest Proxy
$ ./bin/kafka-rest-start ./etc/kafka-rest/kafka-rest.properties

# Create all required topics
$ bin/kafka-topics.sh --create --zookeeper <ZOOKEEPER-HOST>:<ZOOKEEPER-PORT> --replication-factor 1 --partitions 1 --topic <TOPIC-NAME>
```

## Radar application

This is a Gradle project, to execute the following step
- open your shell inside the project folder
```shell
# Clean
$ gradle clean

# Build
$ gradle jar
```
- copy your jar in your server
- run your jar
```shell
$ java -jar radarbackend-1.0.jar <LOG-FILE> <NUM-THREAD>
```

### NOTE
- if you have installed the Confluent Platform via `apt`, the base path will be `/usr/bin/` instead of `bin/`
- if you have installed the Confluent Platform via `sudo` all commands have to be run by a sudoers user 
- commands to start Zookeeper, Kafka, Schema Registry and Rest Proxy have to be run one per shell
- if you have not modified the out-of-the-box configuration files of Confluent `<ZOOKEEPER-HOST>:<ZOOKEEPER-PORT>` values `localhost:2181`
- the `radarbackend-<VERSION>.jar` file is located in `/project-root/build/lib`
- `<LOG-FILE>` and `<NUM-THREAD>` are optional, if not provided the application starts in standalone mode and creates a log file in the same folder of the jar file. Possible alternatives are
```shell
# Default
$ java -jar radarbackend-1.0.jar

# Custom
$ java -jar radarbackend-1.0.jar /Users/francesco/Desktop/radar-test/log/fra.log 1
```
- if `AUTO.CREATE.TOPICS.ENABLE` is `FALSE` in your server.properties, before starting you must create the following topics manually: 
  - android_empatica_e4_acceleration
  - android_empatica_e4_acceleration_output
  - android_empatica_e4_battery_level
  - android_empatica_e4_battery_level_output
  - android_empatica_e4_blood_volume_pulse
  - android_empatica_e4_blood_volume_pulse_output
  - android_empatica_e4_electrodermal_activity
  - android_empatica_e4_electrodermal_activity_output
  - android_empatica_e4_heartrate_output
  - android_empatica_e4_inter_beat_interval
  - android_empatica_e4_inter_beat_interval_output
  - android_empatica_e4_sensor_status
  - android_empatica_e4_sensor_status_output
  - android_empatica_e4_temperature
  - android_empatica_e4_temperature_output