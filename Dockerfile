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

FROM gradle:6.6.1-jdk11 as builder

RUN mkdir /code
WORKDIR /code

ENV GRADLE_OPTS=-Dorg.gradle.project.profile=prod \
  GRADLE_USER_HOME=/code/.gradlecache

COPY ./gradle/profile.prod.gradle /code/gradle/
COPY ./build.gradle ./gradle.properties ./settings.gradle /code/

RUN gradle downloadRuntimeDependencies

COPY ./src/ /code/src

RUN gradle distTar && \
  tar xf build/distributions/*.tar && \
  rm build/distributions/*.tar

FROM openjdk:11-jre-slim

MAINTAINER Nivethika M <nivethika@thehyve.nl> , Joris Borgdorff <joris@thehyve.nl> , Yatharth Ranjan <yatharth.ranjan@kcl.ac.uk>

LABEL description="RADAR-CNS Backend streams and monitor"

RUN apt-get update && apt-get install -y --no-install-recommends \
		curl \
		wget \
	&& rm -rf /var/lib/apt/lists/*

ENV KAFKA_REST_PROXY http://rest-proxy:8082
ENV KAFKA_SCHEMA_REGISTRY http://schema-registry:8081

COPY --from=builder /code/radar-backend-*/bin/* /usr/bin/
COPY --from=builder /code/radar-backend-*/lib/* /usr/lib/

CMD ["radar-backend", "-c", "/etc/radar.yml"]
