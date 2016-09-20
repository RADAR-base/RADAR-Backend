# RADAR-CNS backend

A backend using Apache Kafka to process streaming data from multiple devices. It requires Java to run. In the `org.radarcns.collect` package the classes produce data for the Kafka framework to process. In the `src/resources` directory the Avro schemas used to generate data are stored.

## Usage

Run the application with
```shell
./gradlew run
```

