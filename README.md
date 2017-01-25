# RADAR-CNS back-end

[![Build Status](https://travis-ci.org/RADAR-CNS/RADAR-Backend.svg?branch=master)](https://travis-ci.org/RADAR-CNS/RADAR-Backend)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/e21c0c25f43e4676a3c69bae444101ca)](https://www.codacy.com/app/RADAR-CNS/RADAR-Backend?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RADAR-CNS/RADAR-Backend&amp;utm_campaign=Badge_Grade)

RADAR-Backend is a Java application based on Confluent Platform to standardize, analyze and persist data collected by RADAR-CNS data sources. It supports the backend requirements of RADAR-CNS project. The data is produced and consumed in Apache Avro format using the schema stored inside the RADAR-CNS [schema repository](https://github.com/RADAR-CNS/RADAR-Schemas).

RADAR-Backend provides an abstract layer to monitor and analyze streams of wearable data and write data to Hot or Cold storage. The Application Programming Interfaces (APIs) of RADAR-Backend makes the process of to integrating additional topics, wearable devices easier. It currently provides MongoDB as the Hot-storage and HDFS data store as the Cold-storage. They can be easily tuned using property files. The stream-monitors monitor topics and notify users (e.g. via emails) under given circumstances. 

## Dependencies

The following are the prerequisites to run RADAR-Backend on your machine:

   - Java 8 installed
   - [Confluent Platform 3.1.0](http://docs.confluent.io/3.1.0/installation.html) ( Running instances of Zookeeper, Kafka-broker(s), Schema-Registry and Kafka-REST-Proxy services ).
   - MongoDB installed and running ( to use Hot-storage )
   - Hadoop 2.7.3 and HDFS installed and configured ( to use Cold-storage )
   - SMTP server

## Extending abstract APIs

### Extending KafkaStreams 
The implementation of KafkaStreams is generalized using abstract layers. 

#####Topics
The stream topics are categorized into two categories. These topics are a extensions of [AvroTopic<K,V>](https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/topic/sensor/AvroTopic.java). 
- [SensorTopic<V>][5]: Defines a topic that can be consumed and aggregated as it is.
- [InternalTopic<O>][6]: Defines a topic used to consume,aggregate and transform.
- [SensorTopics][7]: Contains the list of SensorTopic<V>s.
- [InternalTopics][8]: Contains the list of InternalTopic<O>s.

[SensorTopics][7] and [InternalTopics][8] allows the implementation for sets of specific topic type. To improve the flexibility, these topic sets are then unified by [DeviceTopics][9].
#####Streams
KafkaStreams currently communicates using master-slave model. The [MasterAggregator][1] defines the stream-master, while [AggregatorWorker][2] represents the stream-slave. The master-stream creates, starts and stops a list of stream-slaves registered with the corresponding master. 
There are two types of slave-streams ( i.e. AggregatorWorker ):
- [SensorAggregator][3]: Consumes and aggregates topics without performing any transformations
- [InternalAggregator][4] Modifies incoming streams and produces a new information
The SensorAggregator consumes [SensorTopic<V>][5], while the InternalAggregator reads [InternalTopic<O>][6].
While the classical Kafka Consumer requires two implementations to support standalone and group executions, the AggregatorWorker abstraction provides both behaviors with one implementation.

### To extend RADAR-Backend for additional devices
Follow these steps (see the `org.radarcns.empatica` package as an example):
- Create [SensorTopics][7] and if needed [InternalTopics][8] then unify them in [DeviceTopics][9]
- For each topic, create either [SensorAggregator][3] or [InternalAggregator][4]
- Define the [MasterAggregator][1]

## Empatica E4

Currently, RADAR-Backend provides implementation to stream, monitor, store Empatica E4 topics data produced by RADAR-AndroidApplication. 
[E4Worker][11] is the [MasterAggregator][1]. [E4SensorTopics][12] and [E4InternalTopics][13] are respectively [SensorTopics][7] and [InternalTopics][8]. [E4Topics][14] (i.e. [DeviceTopics][9]) are consumed by:
- [E4Acceleration][15]: it aggregates data coming from accelerometer
- [E4BatteryLevel][16]: it aggregates battery level information
- [E4BloodVolumePulse][17]: it aggregates blood volume pulse data
- [E4ElectroDermalActivity][18]: it aggregates electrodermal activity informations
- [E4HeartRate][19]: starting from the inter beat interval, this aggregator computes the heart rate value  (i.e. [InternalAggregator][4])
- [E4InterBeatInterval][20]: it aggregates inter beat interval data
- [E4Temperature][21]: it aggregates data coming form temperature sensor

[DeviceTimestampExtractor][10] implements a [TimestampExtractor](http://docs.confluent.io/3.1.0/streams/javadocs/index.html) such that: given in input a generic APACHE Avro object, it extracts a field named `timeReceived`. [DeviceTimestampExtractor][10] works with the entire set of sensor schemas currently available.

## Installing and Running RADAR-Backend components

### To Build the RADAR-Backend from source, (Check the prerequisites mentioned above):

   1. Clone RADAR-Backend repository.
    ```shell
    git clone https://github.com/RADAR-CNS/RADAR-Backend.git
    ```
    
   2. Build the project from project directory
    ```shell
    # Navigate to project directory
    cd RADAR-Backend/
    
    # Clean
    ./gradlew clean
    
    # Build
    ./gradlew build
    ```
   Build process creates separate jar files for each component. Built jars are located under `/build/libs` folder
   
To Run Radar-backend streams    
   1. Start the Zookeeper, Kafka-broker(s), Schema-registry and Rest-proxy.
   2. Configure `radar.yml` accordingly.
    1. Specify in which `mode` you want to run the application. 
      There are two alternatives, such as `standalone` and `high_performance`. The `standalone` starts one thread for each streams without checking the priority, whereas the `high_performance` starts as many thread as the related priority value
    2. Insert Zookeeper server, Kafka-broker, Schema-registry information
    3. [OPTIONAL] Provide a custom `log_path` ( It must be a folder)
    4. [OPTIONAL] if emails should be sent when the battery level is critical, add the property
    5. If `auto.create.topics.enable` is `false` in your `server.properties`, before starting you must create the topics manually. Create the following topics for Empatica E4 Streams
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
    6. Copy radarbackend.jar and radar.yml in prefered location
    7. Run `radarbackend.jar` with configured `radar.yml` and `stream` argument    
       ```shell
       java -jar radarbackend-1.0.jar -c path/to/radar.yml stream
       ```
       
### To Run Radar-backend monitors
   1. Follow the first 6 steps mentioned above. 
   2. In addition to the changes mentioned above in `radar.yml`, configure required monitor settings. (To get email notifications for Empatica E4 battery status, an email server has to be setup on `localhost`.)
      1. For battery status monitor, configure the following
        ```yaml
        battery_monitor:
        # level of battery you want to monitor
           level: CRITICAL 
        # list of email addresses to be notified   
           email_address:
             - notify-me@example.com
        # host name of your email server     
           email_host: localhost
        # port of email server   
           email_port: 25
        # notifying email account   
           email_user: noreply@example.com
        # list of topics to be monitored ( related to monitor behavior)   
           topics:
             - android_empatica_e4_battery_level
        ```
        
      2. For device connection monitor, configure the following
        ```yaml
        disconnect_monitor:
           # timeout in milliseconds -> 5 minutes
           timeout: 300000
           email_address: 
             - notify-me@example.com
           email_host: localhost
           email_port: 25  
           email_user: noreply@example.com
           # temperature readings are sent very regularly, but
           # not too often.
           topics:
             - android_empatica_e4_temperature
        ```
        
      3. Run `radarbackend.jar` with configured `radar.yml` and `monitor` argument
         ```shell
         java -jar radarbackend-1.0.jar -c path/to/radar.yml monitor
         ```
         
### To Run Radar-HDFS-Connector
   1. In addition to Zookeeper, Kafka-broker(s), Schema-registry and Rest-proxy, HDFS should be running
   2. Load the `radar-hdfs-connector-0.1.jar` to CLASSPATH
   ```shell
   export CLASSPATH=/path/to/radar-hdfs-connector-0.1.jar
   ```
      
   2. Configure HDFS Connector properties.
   ```ini
   name=radar-hdfs-sink
   connector.class=io.confluent.connect.hdfs.HdfsSinkConnector
   tasks.max=1
   topics=mock_empatica_e4_battery_level,mock_empatica_e4_blood_volume_pulse
   flush.size=1200
   hdfs.url=hdfs://localhost:9000
   format.class=org.radarcns.sink.HDFS.AvroFormatRadar
   ```
   For more details visit our [wiki](https://github.com/RADAR-CNS/RADAR-Backend/wiki/Guide-to-RADAR-HDFS-Connector) and [Kafka-HDFS-Connector](http://docs.confluent.io/3.1.0/connect/connect-hdfs/docs/hdfs_connector.html)
   
   3. Run the connector. To run the connector in `standalone mode` (on an enviornment confluent platform is installed)
   
   ```shell
   connect-standalone /etc/schema-registry/connect-avro-standalone.properties path-to-your-hdfs-connector.properties
   ```
   
### To Run Radar-MongoDB-Connector
   1. In addition to Zookeeper, Kafka-broker(s), Schema-registry and Rest-proxy, MongoDB should be running with required credentials
   2. Load the `radar-mongodb-connector-0.1.jar` to CLASSPATH
   ```ini
   export CLASSPATH=/path/to/radar-mongodb-connector-0.1.jar
   ```
      
   2. Configure MongoDB Connector properties.
   ```ini
  # Kafka consumer configuration
   name=radar-connector-mongodb-sink

   # Kafka connector configuration
   connector.class=org.radarcns.mongodb.MongoDbSinkConnector
   tasks.max=1

   # Topics that will be consumed
   topics=topics

   # MongoDB server
   mongo.host=localhost
   mongo.port=27017

   # MongoDB configuration
   mongo.username=
   mongo.password=
   mongo.database=

   # Collection name for putting data into the MongoDB database. The {$topic} token will be replaced
   # by the Kafka topic name.
   #mongo.collection.format={$topic}

   # Factory class to do the actual record conversion
   record.converter.class=org.radarcns.sink.mongodb.RecordConverterFactoryRadar
   ```
   For more details visit our [MongoDBConnector](https://github.com/RADAR-CNS/RADAR-MongoDbConnector) and [Kafka-Connect](http://docs.confluent.io/3.1.0/connect/quickstart.html)
   
   3. Run the connector. To run the connector in `standalone mode` (on an enviornment confluent platform is installed)
   
   ```shell
   connect-standalone /etc/schema-registry/connect-avro-standalone.properties path-to-your-mongodb-connector.properties
   ```   
   
## Contributing
Code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html).
If you want to contribute a feature or fix browse our [issues](https://github.com/RADAR-CNS/RADAR-Backend/issues), and please make a pull request.

### NOTE

- the `radarbackend-<VERSION>.jar` file is located in `build/libs`
- Another path to the YAML configuration file can be given with the `-c` flag:
    ```shell
    # Custom
    $ java -jar radarbackend-1.0.jar -c path/to/radar.yml
    ```
- the default log path is the jar folder
- threads priorities value must be bigger than 0

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
