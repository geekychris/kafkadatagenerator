# Build stage
FROM maven:3.8-openjdk-11 AS build
WORKDIR /app
COPY pom.xml .
# Download dependencies separately for better caching
RUN mvn dependency:go-offline
COPY src ./src
RUN mvn package -DskipTests

# Runtime stage
FROM openjdk:11-jre-slim
WORKDIR /app

# Copy the JAR file from the build stage
COPY --from=build /app/target/datagenerator.jar /app/data-generator.jar

# Create volume mount point for data
VOLUME /data

# Set up labels
LABEL maintainer="Data Generator Team"
LABEL version="1.0"
LABEL description="Data generator application for reading CSV files and publishing to Kafka"

# Set entrypoint script
ENTRYPOINT ["java", "-jar", "/app/data-generator.jar"]

# Default command (can be overridden)
CMD ["--help"]

