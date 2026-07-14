# STAGE 1: Build CLI JAR
FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests

# STAGE 2: Runtime
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

COPY --from=builder /app/target/minicache-cli-*.jar app.jar

RUN echo '#!/bin/sh' > /usr/local/bin/minicache-cli && \
    echo 'exec java -jar /app/app.jar "${CORE_HOST}" "${CORE_PORT}" "$@"' >> /usr/local/bin/minicache-cli && \
    chmod +x /usr/local/bin/minicache-cli

CMD ["sleep", "infinity"]