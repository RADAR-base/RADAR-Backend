# RADAR-CNS back-end

[![Build Status](https://travis-ci.org/RADAR-base/RADAR-Backend.svg?branch=master)](https://travis-ci.org/RADAR-base/RADAR-Backend)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/e21c0c25f43e4676a3c69bae444101ca)](https://www.codacy.com/app/RADAR-base/RADAR-Backend?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=RADAR-CNS/RADAR-Backend&amp;utm_campaign=Badge_Grade)

RADAR-Backend is a Java application based on Confluent Platform to standardize, analyze and persist
data collected by RADAR-CNS data sources. It supports the backend requirements of RADAR-CNS project.
The data is produced and consumed in Apache Avro format using the schema stored inside the
RADAR-CNS [schema repository](https://github.com/RADAR-base/RADAR-Schemas).

RADAR-Backend provides an abstract layer to monitor and analyze streams of wearable data and write
data to Hot or Cold storage. The Application Programming Interfaces (APIs) of RADAR-Backend makes
the process of to integrating additional topics, wearable devices easier. It currently provides
MongoDB as the Hot-storage and HDFS data store as the Cold-storage. They can be easily tuned using
property files. The stream-monitors monitor topics and notify users (e.g. via emails) under given
circumstances.

## Dependencies

The following are the prerequisites to run RADAR-Backend on your machine:

- Java 8
- [Confluent Platform 5.0.0](http://docs.confluent.io/5.0.0/installation.html) ( Running instances
  of Zookeeper, Kafka-broker(s), Schema-Registry and Kafka-REST-Proxy services ).
- SMTP server to send notifications from the monitors.

## Installation

1. Install the dependencies mentioned above.
2. Clone radar-backend repository.

    ```shell
    git clone https://github.com/RADAR-base/radar-backend.git
    ```
3. Build the project from project directory

    ```shell
    # Navigate to project directory
    cd RADAR-Backend/
    
    # Clean
    ./gradlew clean
    
    # Build
    ./gradlew distTar
    
    # Unpack the binaries
    sudo mkdir -p /usr/local && \
    sudo tar --strip-components 1 -C /usr/local xzf build/distributions/*.tar.gz
    ```

   Now the backend is available as the `/usr/local/bin/radar-backend` script.

## Usage

The RADAR command-line has three subcommands: `stream`, `monitor` and `mock`. The `stream` command
will start all streams, the `monitor command` will start all monitors, and the `mock` command will
send mock data to the backend. Before any of these commands are issued, start the Confluent platform
with the zookeeper, kafka, schema-registry and rest-proxy components. Put
the `build/libs/radarbackend-1.0.jar` and `radar.yml` in the same folder, and then
modify `radar.yml`:

### RADAR-Backend streams

1. In `radar.yml`, Specify in which `mode` you want to run the application. There are two
   alternatives: `standalone` and `high_performance`. The `standalone` starts one thread for each
   streams without checking the priority, whereas the `high_performance` starts as many thread as
   the related priority value
2. If `auto.create.topics.enable` is `false` in your Kafka `server.properties`, before starting you
   must create the topics manually. The stream server will print what topics to create.
3. Run `radar-backend` with configured `radar.yml` and `stream` argument

    ```shell
    radar-backend -c path/to/radar.yml stream
    ```

The phone usage event stream uses an internal cache of 1 million elements, which may take about 50
MB of memory. Adjust `org.radarcns.stream.phone.PhoneUsageStream.MAX_CACHE_SIZE` to change it.

### RADAR-backend monitors

To get email notifications for Empatica E4 battery status, an email server without a password set
up, for example on `localhost`.

1. For battery status monitor, configure the following

    ```yaml
    battery_monitor:
      # level of battery you want to monitor
      level: CRITICAL 
      # list of email addresses to be notified   
      notify:
        - project_id: s1
          email_address:
            - test@thehyve.nl
        - project_id: s2
          email_address:
            - radar@thehyve.nl
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
      email_host: localhost
      email_port: 25
      email_user: no-reply@example.com
      notify:
        - project_id: s1
          email_address:
            - test@thehyve.nl
        - project_id: s2
          email_address:
            - radar@thehyve.nl
      # temperature readings are sent very regularly, but
      # not too often.
      topics:
        - android_empatica_e4_temperature
      ```

3. For Source Statistics monitors, configure what source topics to monitor to output some basic
   output statistics (like last time seen)

    ```yaml
    stream:
      statistics_monitors:
        # Human readable monitor name
        - name: Empatica E4
          # topics to aggregate. This can take any number of topics that may
          # lead to slightly different statistics
          topics:
            - android_empatica_e4_blood_volume_pulse_1min
          # Topic to write results to. This should follow the convention
          # source_statistics_[provider]_[model] with produer and model as
          # defined in RADAR-Schemas
          output_topic: source_statistics_empatica_e4
          # Maximum batch size to aggregate before sending results.
          # Defaults to 1000.
          max_batch_size: 500
          # Flush timeout in milliseconds. If the batch size is not larger than
          # max_batch_size for this amount of time, the current batch is
          # forcefully flushed to the output topic.
          # Defaults to 60000 = 1 minute.
          flush_timeout: 15000
        - name: Biovotion VSM1
          topics:
            - android_biovotion_vsm1_acceleration_1min
          output_topic: source_statistics_biovotion_vsm1
        - name: RADAR pRMT
          topics:
            - android_phone_acceleration_1min
            - android_phone_bluetooth_devices
            - android_phone_sms
          output_topic: source_statistics_radar_prmt
    ```

3. Run `radar-backend` with configured `radar.yml` and `monitor` argument

    ```shell
    radar-backend -c path/to/radar.yml monitor
    ```

### RADAR-backend Realtime Inference Consumers

Realtime Consumers are consumers for the results from the invocation of a model from the realtime
analysis pipeline (
see [model-builder](https://github.com/RADAR-base/model-builder/tree/dev/model-builder)
, [model-invocation-endpoint](https://github.com/RADAR-base/model-builder/tree/dev/model-invocation-endpoint)
and [ksqldb](https://github.com/RADAR-base/ksql-extras)). The results from the pipeline are in JSON
and the invocation is called by
a [KSQL function](https://github.com/RADAR-base/ksql-extras/blob/main/src/main/java/org/radarbase/ksql/udf/RestInferenceUdf.java)
which gets inference results then puts the results in a kafka topic. These consumers are meant to
consume data from this topic.

3 terms used -

1. `Action`: Any task to be performed is an action. This can be sending a notification or an email
   or just logging something.
2. `Condition`: This is a predicate that will be evaluated on the incoming data. For instance, this
   can be used to evaluate if the ML model run crossed a threshold of some sort.
3. `RealtimeInferenceConsumer`: This is the base consumer for realtime results topics. Each consumer
   can be configured with a set of Conditions and Actions. If all the conditions evaluate to be
   true, then all the actions are triggered.

Currently, only one Condition is provided which evaluates
a [JSONPath](https://github.com/json-path/JsonPath) expression. This should be sufficient for most
simple use cases. More complex use cases can create concrete implementations of the `Condition`
interface or `ConditionBase` abstract class. Currently, the JsonPath expression is read from the
configuration file and hence is static for a particular consumer and topic. Later, we can also
provide this through AppConfig so it can be dynamic based on userId or projectId.

The supported config (for instance an intervention using aRMT app) is of the format -

```yaml
realtime-consumers:
  - name: 'lstm-lung-study-consumer' # Name of the consumer
    topic: 'lung_study_lstm_ad_inference' # Kafka topic to consume from (this should contain the inference results)
    notify_errors:
      email_addresses:
        - 'admin@radar-base.org'
    conditions:
      - name: 'LocalJsonPathCondition' # Name of the condition
        projects: [ 'radar-test' ]  # Only evaluate for these projects
        subjects: [ 'sub-1', 'sub-2' ]  # Only evaluate for these subjects
        properties:
          jsonpath: '$[?(@.invocation_result.anomaly_detected == true)]' # JsonPath expression to evaluate
          key: 'invocation_result' # Key that contains data to evaluate 
    actions:
      - name: 'ActiveAppNotificationAction' # Name of the action
        projects: [ 'radar-test' ] # Only execute for these projects
        subjects: [ 'sub-1', 'sub-2' ]  # Only execute for these subjects
        properties:
          questionnaire_name: 'ers' # Name of the questionnaire to trigger
          time_of_day: '09:00:00' # Local user time of day to trigger at
          default_timezone: 'Europe/London' # Default timezone to use for the time of day if not found in the appserver
          appserver_base_url: 'http://localhost:8080/' # Base URL of the appserver
          management_portal_token_url: 'http://localhost/managementportal/api/oauth/token' # URL to get the management portal token
          client_id: 'realtime_consumer' # Client ID for the management portal
          client_secret: 'secret' # Client secret for the management portal
          metadata_key: 'invocation_result' # Key that contains the metdata to be forwarded to the aRMT app
      - name: 'EmailUserAction'
        projects: [ 'radar-test' ]
        subjects: [ 'sub-1', 'sub-2' ]
        properties:
          email_addresses: [ 'admin@radar-base.org' ]
```

Note: The `properties` section is specific to each Action and Condition. Please take a look at the
condition and action docs for the keys supported. If the `projects` or `subjects` key is not
specified the action and condition will be used on all projects or subjects respectively.

### Send mock data to the backend

1. Configure the REST proxy setting in `radar.yml`:

    ```yaml
    rest-proxy:
      host: radar-test.thehyve.net
      port: 8082
      protocol: http
    ```

2. To send pre-made data, create a `mock_data.yml` YAML file with the following contents:

    ```yaml
    data:
      - topic: topic1
        file: topic1.csv
        key_schema: org.radarcns.kafka.ObservationKey
        value_schema: org.radarcns.passive.empatica.EmpaticaE4Acceleration
    ```

   Each value has a topic to send the data to, a file containing the data, a schema class for the
   key and a schema class for the value. Also create a CSV file for each of these entries:
    ```csv
    userId,sourceId,time,timeReceived,acceleration
    a,b,14191933191.223,14191933193.223,[0.001;0.3222;0.6342]
    a,c,14191933194.223,14191933195.223,[0.13131;0.6241;0.2423]
    ```
   Note that for array entries, use brackets (`[` and `]`) to enclose the values and use `;` as a
   delimiter.

3. To generate data on some `backend_mock_empatica_e4_<>` topic with a number of devices, run (
   substitute `<num-devices>` with the needed number of devices):

    ```shell
    radar-backend -c path/to/radar.yml mock --devices <num-devices>
    ```

   Press `Ctrl-C` to stop.

4. To generate the file data configured in point 2, run

    ```shell
    radar-backend -c path/to/radar.yml mock --file mock_data.yml
    ```

   The data sending will automatically be stopped.

### Docker image

The backend is [published to Docker Hub](https://hub.docker.com/r/radarcns/radar-backend-kafka).
Mount a `/etc/radar.yml` file to configure either the streams or the monitor.

This image requires the following environment variable:

- `KAFKA_REST_PROXY`: a valid Rest-Proxy instance
- `KAFKA_SCHEMA_REGISTRY`: a valid Confluent Schema Registry.
- `KAFKA_BROKERS`: number of brokers expected (default: 3).

For a complete use case scenario, check the RADAR-base `docker-compose` file
available [here](https://github.com/RADAR-base/RADAR-Docker/blob/backend-integration/dcompose-stack/radar-cp-hadoop-stack/docker-compose.yml)

## Contributing

Code should be formatted using
the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html). If you want
to contribute a feature or fix browse
our [issues](https://github.com/RADAR-base/RADAR-Backend/issues), and please make a pull request.

There are currently two APIs in RADAR-Backend: one for streaming data (RADAR-Stream) and one for
monitoring topics (RADAR-Monitor). To contribute to those APIs, please mind the following.

### Extending RADAR-Stream

RADAR-Stream is a layer on top of Kafka streams. Topics are processed by streams in two phases.
First, a group of sensor streams aggregates data of sensors into predefined time windows (e.g., 10
seconds). Next, internal topics aggregate and transforms data that has already been processed by an
earlier stream.

KafkaStreams currently communicates using master-slave model. The [StreamMaster][1] defines the
stream-master, while [StreamWorker][2] represents the stream-slave. The master-stream creates,
starts and stops a list of stream-slaves registered with the corresponding master. While the
classical Kafka Consumer requires two implementations to support standalone and group executions,
the StreamWorker provides both behaviors with one implementation.

To extend the RADAR-Stream API, follow these steps (see the `org.radarcns.passive.empatica` package
as an example):

- For each topic, create a [StreamWorker][2] or more conveniently extend [SensorStreamWorker][6].
- Add the stream topic to the `stream: streams: [{class: MyClass}]` configuration

#### Empatica E4

Currently, RADAR-Backend provides implementation to stream, monitor, store Empatica E4 topics data
produced by RADAR-AndroidApplication. It defines the following streams:

- [E4Acceleration][15] aggregates data coming from accelerometer
- [E4BatteryLevel][16] aggregates battery level information
- [E4BloodVolumePulse][17] aggregates blood volume pulse data
- [E4ElectroDermalActivity][18] aggregates electrodermal activity informations
- [E4InterBeatInterval][20] aggregates inter-beat-interval data
- [E4Temperature][21] aggregates data coming form temperature sensor

And one internal topic:

- [E4HeartRate][19]: starting from the inter-beat-interval, this aggregator computes the heart rate

[DeviceTimestampExtractor][10] implements
a [TimestampExtractor](http://docs.confluent.io/5.0.0/streams/javadocs/index.html) such that: given
in input a generic Apache Avro object, it extracts a field named `timeReceived`
. [DeviceTimestampExtractor][10] works with the entire set of sensor schemas currently available.

#### Android Phone

For the Android Phone, there is a stream to get an app category from the Google Play Store
categories for app usage events.

### Extending RADAR-Monitor

Monitors can be used to evaluate the status of a single stream, for example whether each device is
still online, has acceptable values and is transmitting at an acceptable rate. To create a new
monitor, extend [AbstractKafkaMonitor][3]. To use the monitor from the command-line,
modify [KafkaMonitorFactory][4]. See [DisconnectMonitor][5] for an example.

### NOTE

- Another path to the YAML configuration file can be given with the `-c` flag:

    ```shell
    # Custom
    java -jar radarbackend-1.0.jar -c path/to/radar.yml
    ```
- the default log path is the jar folder

[1]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/StreamMaster.java

[2]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/StreamWorker.java

[3]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/monitor/AbstractKafkaMonitor.java

[4]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/monitor/KafkaMonitorFactory.java

[5]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/monitor/DisconnectMonitor.java

[6]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/SensorStreamWorker.java

[10]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/DeviceTimestampExtractor.java

[15]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/empatica/E4Acceleration.java

[16]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/empatica/E4BatteryLevel.java

[17]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/empatica/E4BloodVolumePulse.java

[18]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/empatica/E4ElectroDermalActivity.java

[19]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/empatica/E4HeartRate.java

[20]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/empatica/E4InterBeatInterval.java

[21]: https://github.com/RADAR-base/RADAR-Backend/blob/master/src/main/java/org/radarcns/stream/empatica/E4Temperature.java
