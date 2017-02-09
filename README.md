# RADAR-CNS back-end

[![Build Status](https://travis-ci.org/RADAR-CNS/RADAR-Backend.svg?branch=master)](https://travis-ci.org/RADAR-CNS/RADAR-Backend)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/e21c0c25f43e4676a3c69bae444101ca)](https://www.codacy.com/app/RADAR-CNS/RADAR-Backend?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RADAR-CNS/RADAR-Backend&amp;utm_campaign=Badge_Grade)

RADAR-Backend is a Java application based on Confluent Platform to standardize, analyze and persist data collected by RADAR-CNS data sources. It supports the backend requirements of RADAR-CNS project. The data is produced and consumed in Apache Avro format using the schema stored inside the RADAR-CNS [schema repository](https://github.com/RADAR-CNS/RADAR-Schemas).

RADAR-Backend provides an abstract layer to monitor and analyze streams of wearable data and write data to Hot or Cold storage. The Application Programming Interfaces (APIs) of RADAR-Backend makes the process of to integrating additional topics, wearable devices easier. It currently provides MongoDB as the Hot-storage and HDFS data store as the Cold-storage. They can be easily tuned using property files. The stream-monitors monitor topics and notify users (e.g. via emails) under given circumstances. 

## Dependencies

The following are the prerequisites to run RADAR-Backend on your machine:

- Java 8
   - [Confluent Platform 3.1.0](http://docs.confluent.io/3.1.0/installation.html) ( Running instances of Zookeeper, Kafka-broker(s), Schema-Registry and Kafka-REST-Proxy services ).
- MongoDB installed and running ( to use Hot-storage )
- Hadoop 2.7.3 and HDFS installed and configured ( to use Cold-storage )
- SMTP server to send notifications from the monitors.

## Installation

1. Install the dependencies mentioned above.
2. Clone RADAR-Backend repository.
    
    ```shell
    git clone https://github.com/RADAR-CNS/RADAR-Backend.git
    ```
3. Build the project from project directory

    ```shell
    # Navigate to project directory
    cd RADAR-Backend/
    
    # Clean
    ./gradlew clean
    
    # Build
    ./gradlew build
    ```
   The build process creates separate jar files for each component. Built jars are located under `/build/libs` folder.


## Usage

The RADAR command-line has three subcommands: `stream`, `monitor` and `mock`. The `stream` command will start all streams, the `monitor command` will start all monitors, and the `mock` command will send mock data to the backend. Before any of these commands are issued,  start the Confluent platform with the zookeeper, kafka, schema-registry and rest-proxy components. Put the `build/libs/radarbackend-1.0.jar` and `radar.yml` in the same folder, and then modify `radar.yml`:

1. Insert Zookeeper server, Kafka-broker, Schema-registry information
2. [OPTIONAL] Provide a custom `log_path` ( It must be a folder)


### RADAR-Backend streams

1. In `radar.yml`, Specify in which `mode` you want to run the application. There are two alternatives: `standalone` and `high_performance`. The `standalone` starts one thread for each streams without checking the priority, whereas the `high_performance` starts as many thread as the related priority value
2. If `auto.create.topics.enable` is `false` in your Kafka `server.properties`, before starting you must create the topics manually. Create the following topics for Empatica E4 Streams
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
3. Run `radarbackend.jar` with configured `radar.yml` and `stream` argument    

    ```shell
    java -jar radarbackend-1.0.jar -c path/to/radar.yml stream
    ```
       
### RADAR-backend monitors

To get email notifications for Empatica E4 battery status, an email server without a password set up, for example on `localhost`.

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

### Send mock data to the backend
 
1. Configure the REST proxy setting in `radar.yml`:

    ```yaml
    rest-proxy:
      host: radar-test.thehyve.net
      port: 8082
      protocol: http
    ```
2. To generate data on some `backend_mock_empatica_e4_<>` topic with a number of devices, run (substitute `<num-devices>` with the needed number of devices):

    ```shell
    java -jar radarbackend-1.0.jar -c path/to/radar.yml mock --devices <num-devices>
    ```
        
    Press `Ctrl-C` to stop.

### To Run Radar-HDFS-Connector

1. In addition to Zookeeper, Kafka-broker(s), Schema-registry and Rest-proxy, HDFS should be running
2. Load the `radar-hdfs-connector-0.1.jar` to CLASSPATH

    ```shell
    export CLASSPATH=/path/to/radar-hdfs-connector-0.1.jar
    ```
      
3. Configure HDFS Connector properties.

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
   
4. Run the connector. To run the connector in `standalone mode` (on an enviornment confluent platform is installed)
   
    ```shell
    connect-standalone /etc/schema-registry/connect-avro-standalone.properties path-to-your-hdfs-connector.properties
    ```
   
### To Run Radar-MongoDB-Connector

1. In addition to Zookeeper, Kafka-broker(s), Schema-registry and Rest-proxy, MongoDB should be running with required credentials
2. Load the `radar-mongodb-connector-0.1.jar` to CLASSPATH
    
    ```ini
    export CLASSPATH=/path/to/radar-mongodb-connector-0.1.jar
    ```
      
3. Configure MongoDB Connector properties.

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
   
4. Run the connector. To run the connector in `standalone mode` (on an enviornment where the Confluent platform is installed):
   
    ```shell
    connect-standalone /etc/schema-registry/connect-avro-standalone.properties path-to-your-mongodb-connector.properties
    ```   
   
## Contributing
Code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html).
If you want to contribute a feature or fix browse our [issues](https://github.com/RADAR-CNS/RADAR-Backend/issues), and please make a pull request.

There are currently two APIs in RADAR-Backend: one for streaming data (RADAR-Stream) and one for monitoring topics (RADAR-Monitor). To contribute to those APIs, please mind the following.

### Extending RADAR-Stream

RADAR-Stream is a layer on top of Kafka streams. Topics are processed by streams in two phases. First, an [OutputStreamGroup][7] aggregates data of sensors into predefined time windows (e.g., 10 seconds). Next, a [GeneralStreamGroup][8] aggregates and transforms data that has already been processed by an earlier stream.

KafkaStreams currently communicates using master-slave model. The [MasterAggregator][1] defines the stream-master, while [AggregatorWorker][2] represents the stream-slave. The master-stream creates, starts and stops a list of stream-slaves registered with the corresponding master. 
While the classical Kafka Consumer requires two implementations to support standalone and group executions, the AggregatorWorker provides both behaviors with one implementation.

To extend the RADAR-Stream API, follow these steps (see the `org.radarcns.empatica` package as an example):
- Create sensor streams with an [OutputStreamGroup][7] and if needed internal streams with a [GeneralStreamGroup][8]. Then unify them in a [CombinedStreamGroup][9].
- For each topic, create a [AggregatorWorker][2].
- Define the [MasterAggregator][1]

#### Empatica E4

Currently, RADAR-Backend provides implementation to stream, monitor, store Empatica E4 topics data produced by RADAR-AndroidApplication. 
[E4Worker][11] is the [MasterAggregator][1]. [E4SensorTopics][12] and [E4InternalTopics][13] are respectively [OutputStreamGroup][7] and [GeneralStreamGroup][8]. [E4Topics][14] (i.e. [CombinedStreamGroup][9]) are consumed by sensor topics:

- [E4Acceleration][15]: it aggregates data coming from accelerometer
- [E4BatteryLevel][16]: it aggregates battery level information
- [E4BloodVolumePulse][17]: it aggregates blood volume pulse data
- [E4ElectroDermalActivity][18]: it aggregates electrodermal activity informations
- [E4InterBeatInterval][20]: it aggregates inter beat interval data
- [E4Temperature][21]: it aggregates data coming form temperature sensor

And internal topic:

- [E4HeartRate][19]: starting from the inter beat interval, this aggregator computes the heart rate value

[DeviceTimestampExtractor][10] implements a [TimestampExtractor](http://docs.confluent.io/3.1.0/streams/javadocs/index.html) such that: given in input a generic Apache Avro object, it extracts a field named `timeReceived`. [DeviceTimestampExtractor][10] works with the entire set of sensor schemas currently available.

### Extending RADAR-Monitor

Monitors can be used to evaluate the status of a single stream, for example whether each device is still online, has acceptable values and is transmitting at an acceptable rate. To create a new monitor, extend [AbstractKafkaMonitor][3]. To use the monitor from the command-line, modify [KafkaMonitorFactory][4]. See [DisconnectMonitor][5] for an example.

### NOTE

- Another path to the YAML configuration file can be given with the `-c` flag:

    ```shell
    # Custom
    java -jar radarbackend-1.0.jar -c path/to/radar.yml
    ```
- the default log path is the jar folder

[1]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/aggregator/MasterAggregator.java
[2]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/aggregator/AggregatorWorker.java
[3]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/monitor/AbstractKafkaMonitor.java
[4]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/monitor/KafkaMonitorFactory.java
[5]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/monitor/DisconnectMonitor.java
[7]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/topic/OutputStreamGroup.java
[8]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/topic/GeneralStreamGroup.java
[9]: https://github.com/RADAR-CNS/RADAR-Backend/blob/master/src/main/java/org/radarcns/topic/CombinedStreamGroup.java
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
