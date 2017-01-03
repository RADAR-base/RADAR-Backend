# RADAR-CNS back-end

[![Build Status](https://travis-ci.org/RADAR-CNS/RADAR-Backend.svg?branch=master)](https://travis-ci.org/RADAR-CNS/RADAR-Backend)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/e21c0c25f43e4676a3c69bae444101ca)](https://www.codacy.com/app/RADAR-CNS/RADAR-Backend?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RADAR-CNS/RADAR-Backend&amp;utm_campaign=Badge_Grade)

A Java application based on Confluent Platform to standardise, analyse and persistent data collected by RADAR-CNS data sources. Data is consumed and produced in Apache Avro format using the schemas stored inside the RADAR-CNS [schema repository](https://github.com/RADAR-CNS/RADAR-Schemas).

Currently only the Empatica E4 is supported.

# Generalisation

The Kafka Streams concept has been generalised. [MasterAggregator][1] defines the stream master while [AggregatorWorker][2] represents the stream slave. The master stream creates, starts and stops a list of stream slave. Two types of slave stream have been defined:
- [SensorAggregator][3] consumes and aggregates topics without performing any transformations
- [InternalAggregator][4] modifies incoming data producing a new information
While the classical Kafka Consumer requires two implementations to support standalone and group executions, the stream slave definition provides both behaviours with one implementation.

While the former consumes [SensorTopic<V>][5], the latter reads [InternalTopic<O>][6]. These topics are a specialisation of [AvroTopic<K,V>]().
- [SensorTopic<V>][5] defines a set of topic used to consume and aggregate data as is
- [InternalTopic<O>][6] delineates a set of topic used to consume and aggregate transformed data
[SensorTopics][7] and [InternalTopics][8] drive the developer to the implementation of sets of topics containing only one topic type. To improve the flexibility, these sets are then unified by [DeviceTopics][9]. All these set should be defined following the [Factory Method Pattern](https://en.wikipedia.org/wiki/Factory_method_pattern). It should be used for each element that has to be unique within the application (e.g. [MasterAggregator][1])

[DeviceTimestampExtractor][10] implements a [TimestampExtractor](http://docs.confluent.io/3.1.0/streams/javadocs/index.html) such that: given in input a generic APACHE Avro object, it extracts a field named `timeReceived`. [DeviceTimestampExtractor][10] works with the entire set of sensor schemas currently available.

## Empatica E4

[E4Worker][11] is the [MasterAggregator][1]. [E4SensorTopics][12] and [E4InternalTopics][13] are respectively [SensorTopics][7] and [InternalTopics][8]. [E4Topics][14] (i.e. [DeviceTopics][9]) are consumed by:
- [E4Acceleration][15]: it aggregates data coming from accelerometer
- [E4BatteryLevel][16]: it aggregates battery level information
- [E4BloodVolumePulse][17]: it aggregates blood volume pulse data
- [E4ElectroDermalActivity][18]: it aggregates electrodermal activity informations
- [E4HeartRate][19]: starting from the inter beat interval, this aggregator computes the heart rate value  (i.e. [InternalAggregator][4])
- [E4InterBeatInterval][20]: it aggregates inter beat interval data
- [E4Temperature][21]: it aggregates data coming form temperature sensor

## Contributing

To add additional devices, make the following steps (see the `org.radarcns.empatica` package as an example):
- create [SensorTopics][7] and if needed [InternalTopics][8] then unify them in [DeviceTopics][9]
- for each topic create either [SensorAggregator][3] or [InternalAggregator][4]
- define your [MasterAggregator][1]

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
1. open your shell inside the project folder
```shell
# Clean
$ gradle clean

# Build
$ gradle jar
```
2. copy your jar in your server
3. modify `radar.yml` file
  1. specify in which mode you want to run the application. The two alternatives are `standalone` and `high_performance`. The former starts one thread for each streams without checking the priority, the latter starts as many thread as the related priority value
  1. insert Zookeeper server information
  2. insert Broker server information
  3. insert Schema-Registry location
  4. insert `log_path`, it must be a folder [OPTIONAL]
4. copy `radar.yml` in the same folder where you have copied the jar
5. run your jar
```shell
$ java -jar radarbackend-1.0.jar <CONFIG-FILE>
```

### NOTE
- if you have installed the Confluent Platform via `apt`, the base path will be `/usr/bin/` instead of `bin/`
- if you have installed the Confluent Platform via `sudo` all commands have to be run by a sudoers user 
- commands to start Zookeeper, Kafka, Schema Registry and Rest Proxy have to be run one per shell
- if you have not modified the out-of-the-box configuration files of Confluent `<ZOOKEEPER-HOST>:<ZOOKEEPER-PORT>` values `localhost:2181`
- the `radarbackend-<VERSION>.jar` file is located in `/project-root/build/lib`
- `<CONFIG-FILE>` is optional, if not provided your config file has to be located in the same folder of your jar
```shell
# Default
$ java -jar radarbackend-1.0.jar

# Custom
$ java -jar radarbackend-1.0.jar /Users/francesco/Desktop/radar-test/config/radar.yml
```
- the default log path is the jar folder
- threads priorities value must be bigger than 0
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

  [1]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/aggregator/MasterAggregator.java
  [2]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/aggregator/AggregatorWorker.java
  [3]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/aggregator/SensorAggregator.java
  [4]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/aggregator/InternalAggregator.java
  [5]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/topic/sensor/SensorTopic.java
  [6]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/topic/Internal/InternalTopic.java
  [7]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/topic/sensor/SensorTopics.java
  [8]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/topic/Internal/InternalTopics.java
  [9]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/topic/device/DeviceTopics.java
  [10]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/aggregator/DeviceTimestampExtractor.java
  [11]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/empatica/E4Worker.java
  [12]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/empatica/topic/E4SensorTopics.java
  [13]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/empatica/topic/E4InternalTopics.java
  [14]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/empatica/topic/E4Topics.java
  [15]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/empatica/streams/E4Acceleration.java
  [16]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/empatica/streams/E4BatteryLevel.java
  [17]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/empatica/streams/E4BloodVolumePulse.java
  [18]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/empatica/streams/E4ElectroDermalActivity.java
  [19]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/empatica/streams/E4HeartRate.java
  [20]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/empatica/streams/E4InterBeatInterval.java
  [21]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/empatica/streams/E4Temperature.java
