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

FROM gradle:6.6.1-jdk11

RUN mkdir /code
WORKDIR /code

ENV GRADLE_OPTS=-Dorg.gradle.project.profile=test \
  GRADLE_USER_HOME=/code/.gradlecache

COPY ./gradle/*test.gradle ./gradle/utilities.gradle /code/gradle/
COPY ./build.gradle ./gradle.properties ./settings.gradle /code/

RUN gradle downloadDependencies

COPY ./src/ /code/src

RUN gradle integrationTestClasses
