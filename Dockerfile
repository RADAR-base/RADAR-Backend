FROM openjdk:8-alpine

ENV TERM=dumb
COPY . /code
WORKDIR /code

RUN ./gradlew downloadDependencies

ENTRYPOINT ["./gradlew"]
