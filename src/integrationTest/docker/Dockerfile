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

FROM openjdk:8

RUN mkdir /code
WORKDIR /code

ENV GRADLE_OPTS -Dorg.gradle.daemon=false -Dorg.gradle.project.profile=test
ENV TERM dumb

COPY ./gradle/wrapper /code/gradle/wrapper
COPY ./gradlew /code/
RUN ./gradlew --version

COPY ./gradle/*test.gradle /code/gradle/
COPY ./build.gradle ./gradle.properties ./settings.gradle /code/

RUN ./gradlew downloadTestDependencies

COPY ./src/ /code/src

RUN ./gradlew integrationTestClasses

ENTRYPOINT ["./gradlew"]
