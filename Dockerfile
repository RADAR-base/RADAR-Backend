# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM gradle:7.3-jdk17 as builder

RUN mkdir /code
WORKDIR /code

ENV GRADLE_OPTS="-Dorg.gradle.project.profile=prod -Djdk.lang.Process.launchMechanism=vfork" \
  GRADLE_USER_HOME=/code/.gradlecache

COPY ./gradle/profile.prod.gradle /code/gradle/
COPY ./build.gradle ./gradle.properties ./settings.gradle /code/

RUN gradle downloadRuntimeDependencies copyDependencies startScripts

COPY ./src/ /code/src

RUN gradle jar

FROM azul/zulu-openjdk-alpine:17-jre-headless

MAINTAINER Nivethika M <nivethika@thehyve.nl> , Joris Borgdorff <joris@thehyve.nl> , Yatharth Ranjan <yatharth.ranjan@kcl.ac.uk>

LABEL description="RADAR-CNS Backend streams and monitor"

RUN apk add --no-cache curl

ENV KAFKA_SCHEMA_REGISTRY http://schema-registry:8081
ENV RADAR_BACKEND_CONFIG /etc/radar.yml

COPY --from=builder /code/build/third-party/* /usr/lib/
COPY --from=builder /code/build/scripts/* /usr/bin/
COPY --from=builder /code/build/libs/* /usr/lib/

# Load topics validator
COPY ./src/main/docker/radar-backend-init /usr/bin

ENTRYPOINT ["radar-backend-init"]
