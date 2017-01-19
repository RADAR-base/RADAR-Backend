FROM java:8

COPY . /code
WORKDIR /code

ENTRYPOINT ["./gradlew"]