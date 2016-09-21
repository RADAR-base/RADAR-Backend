# RADAR-CNS backend

A backend using Apache Kafka to process streaming data from multiple devices. It requires Java to run. In the `org.radarcns.collect` package the classes produce data for the Kafka framework to process. In the `src/resources` directory the Avro schemas used to generate data are stored.

## Usage

Run the application with
```shell
./gradlew run
```

## Contributing

Code should be formatted using the [Google Java Code Style Guide](https://google.github.io/styleguide/javaguide.html). If you want to contribute a feature or fix, please make a pull request
