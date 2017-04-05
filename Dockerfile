FROM openjdk:8-alpine

ENV TERM=dumb
COPY . /code
WORKDIR /code

ENTRYPOINT ["./gradlew"]
