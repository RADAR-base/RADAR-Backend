# RADAR-CNS back-end

[![Build Status](https://travis-ci.org/RADAR-CNS/RADAR-Backend.svg?branch=master)](https://travis-ci.org/RADAR-CNS/RADAR-Backend)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/e21c0c25f43e4676a3c69bae444101ca)](https://www.codacy.com/app/RADAR-CNS/RADAR-Backend?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RADAR-CNS/RADAR-Backend&amp;utm_campaign=Badge_Grade)

RADAR-Backend is a Java application based on Confluent Platform to standardise, analyse and persist data collected by RADAR-CNS data sources. It supports the backend requirements of RADAR-CNS project. The data is produced and consumed in Apache Avro format using the schemas stored inside the RADAR-CNS [schema repository](https://github.com/RADAR-CNS/RADAR-Schemas).

RADAR-Backend provides an abstract layer to monitor and analyse streams of wearable data and write data to Hot or Cold storage. The Application Programming Interfaces (APIs) of RADAR-Backend makes the process of to integrating additional topics, wearable devices easier. It currently provides MongoDB as the Hot-storage and HDFS data store as the Cold-storage. They can be easily tuned using property files. The stream-monitors monitor topics and notify users (e.g. via emails) under given circumstances. 

## Dependencies

The following are the prerequisites to run RADAR-Backend on your machine:

   - Java 8 installed
   - [Confluent Platform 3.1.0](http://docs.confluent.io/3.1.0/installation.html) ( Running instances of Zookeeper, Kafka-broker(s), Schema-Registry and Kafka-REST-Proxy services ).
   - MongoDB installed and running ( to use Hot-storage )
   - Hadoop 2.7.3 and HDFS installed and configured ( to use Cold-storage )
   - SMTP server

## Extending abstract APIs

### Extending KafkaStreams 
The implementation of KafkaStreams is generalised using abstract layers. 

#####Topics
The stream topics are categorized into two categories. These topics are a extensions of [AvroTopic<K,V>](https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/topic/sensor/AvroTopic.java). 
- [SensorTopic<V>][5]: Defines a topic that can be consumed and aggregated as it is.
- [InternalTopic<O>][6]: Defines a topic used to consume,aggregate and transform.
- [SensorTopics][7]: Contains the list of SensorTopic<V>s.
- [InternalTopics][8]: Contains the list of InternalTopic<O>s.

[SensorTopics][7] and [InternalTopics][8] allows the implementation for sets of spefic topic type. To improve the flexibility, these topic sets are then unified by [DeviceTopics][9].
#####Streams
KafkaStreams currently communicates using master-slave model. The [MasterAggregator][1] defines the stream-master, while [AggregatorWorker][2] represents the stream-slave. The master-stream creates, starts and stops a list of stream-slaves registered with the corresponding master. 
There are two types of slave-streams ( i.e. AggregatorWorker ):
- [SensorAggregator][3]: Consumes and aggregates topics without performing any transformations
- [InternalAggregator][4] Modifies incoming streams and produces a new information
The SensorAggregator consumes [SensorTopic<V>][5], while the InternalAggregator reads [InternalTopic<O>][6].
While the classical Kafka Consumer requires two implementations to support standalone and group executions, the AggregatorWorker abstraction provides both behaviours with one implementation.

To extend RADAR-Backend for additional devices, do the following steps (see the `org.radarcns.empatica` package as an example):
- Create [SensorTopics][7] and if needed [InternalTopics][8] then unify them in [DeviceTopics][9]
- For each topic, create either [SensorAggregator][3] or [InternalAggregator][4]
- Define the [MasterAggregator][1]

# Quickstart for Empatica

## Radar application

To run the RADAR applications, run the following:

1. open your shell inside the project folder and runSensorTopics][7]

    ```shell
    # Clean
    ./gradlew clean
    
    # Build
    ./gradlew jar
    ```

2. copy your jar to your server
3. modify the `radar.yml` file
    1. specify in which mode you want to run the application. The two alternatives are `standalone` and `high_performance`. The former starts one thread for each streams without checking the priority, the latter starts as many thread as the related priority value
    2. insert Zookeeper server information
    3. insert Broker server information
    4. insert Schema-Registry location
    5. [OPTIONAL] insert `log_path`, it must be a folder
    6. [OPTIONAL] if emails should be sent when the battery level is critical, add the property
    
        ```yaml
        battery_status:
          - email_address: me@me.me
        ```
        
4. copy `radar.yml` to the same folder where you have copied the jar
5. in that directory, run the aggregator streams with

    ```shell
    java -jar radarbackend-1.0.jar stream
    ```

6. to monitor the battery level and send emails if a device has critical levels, in another shell run 

    ```shell
    java -jar radarbackend-1.0.jar monitor
    ```

### NOTE

- the `radarbackend-<VERSION>.jar` file is located in `build/libs`
- Another path to the YAML configuration file can be given with the `-c` flag:

    ```shell
    # Custom
    $ java -jar radarbackend-1.0.jar -c path/to/radar.yml
    ```

- the default log path is the jar folder
- threads priorities value must be bigger than 0
- if `auto.create.topics.enable` is `false` in your `server.properties`, before starting you must create the following topics manually: 
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

## Contributing



Code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html).

If you want to contribute a feature or fix, please make a pull request




### Empatica E4

[E4Worker][11] is the [MasterAggregator][1]. [E4SensorTopics][12] and [E4InternalTopics][13] are respectively [SensorTopics][7] and [InternalTopics][8]. [E4Topics][14] (i.e. [DeviceTopics][9]) are consumed by:
- [E4Acceleration][15]: it aggregates data coming from accelerometer
- [E4BatteryLevel][16]: it aggregates battery level information
- [E4BloodVolumePulse][17]: it aggregates blood volume pulse data
- [E4ElectroDermalActivity][18]: it aggregates electrodermal activity informations
- [E4HeartRate][19]: starting from the inter beat interval, this aggregator computes the heart rate value  (i.e. [InternalAggregator][4])
- [E4InterBeatInterval][20]: it aggregates inter beat interval data
- [E4Temperature][21]: it aggregates data coming form temperature sensor

[DeviceTimestampExtractor][10] implements a [TimestampExtractor](http://docs.confluent.io/3.1.0/streams/javadocs/index.html) such that: given in input a generic APACHE Avro object, it extracts a field named `timeReceived`. [DeviceTimestampExtractor][10] works with the entire set of sensor schemas currently available.

To get email notifications for Empatica E4 battery status, an email server has to be setup on `localhost`. To [run the HDFS connector](https://github.com/RADAR-CNS/RADAR-Backend/wiki/Guide-to-RADAR-HDFS-Connector), Hadoop needs to be installed. 

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
