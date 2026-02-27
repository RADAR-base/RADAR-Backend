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

# Build stage
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /app

# Copy gradle files
COPY gradlew .
COPY gradle gradle
COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .
COPY gradle/libs.versions.toml gradle/

# Download dependencies
RUN ./gradlew --no-daemon dependencies

# Copy source code
COPY src src

# Build application
RUN ./gradlew --no-daemon installDist

# Run stage
FROM confluentinc/cp-base-new:8.0.4

WORKDIR /app

# Copy built application from build stage
COPY --from=build /app/build/install/kotlin-backend/bin/* /usr/bin
COPY --from=build /app/build/install/kotlin-backend/lib/* /usr/lib

# Load topics validator
COPY ./src/main/docker/radar-backend-init /usr/bin

# Set user to non-root
USER appuser

# Run the application
ENTRYPOINT ["radar-backend-init"]
