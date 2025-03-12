# Data Generator

A Java application that reads CSV files and publishes the data as JSON events to a Kafka topic.

## Project Description

The Data Generator is designed to transform CSV data into JSON records and publish them to Kafka in a configurable manner. This can be useful for:

- Testing Kafka-based applications
- Data migration
- Simulating event streams
- Load testing

The application reads a CSV file, converts each line into a JSON object, and publishes it to a specified Kafka topic. It can be configured to process a specific number of records or the entire file, with an optional pause between records.

## Prerequisites

- Java 11 or later
- Maven 3.6 or later (for building)
- Docker (optional, for containerized deployment)
- Access to Kafka broker(s)

## Build Instructions

### Using Maven

1. Clone the repository
2. Navigate to the project directory
3. Build the project:

```bash
mvn clean package
```

This will create an executable JAR file in the `target` directory.

### Using Docker

Build the Docker image:

```bash
docker build -t data-generator:latest .
```

## Usage Instructions

### Running the JAR directly

```bash
java -jar target/data-generator-1.0-SNAPSHOT.jar \
  --bootstrap-servers <kafka-bootstrap-servers> \
  --topic <kafka-topic> \
  --input-file <path-to-csv-file> \
  --pause-ms <pause-between-records-ms> \
  --num-records <number-of-records-to-process>
```
```bash
java -jar target/datagenerator.jar \
  --bootstrap-servers localhost:9092 \
  --topic data_gen \
  --input-file /Users/chris/code/data_generator/user_behavior_header.log \
  --pause-ms 1000 \
  --num-records 100000
```
### Running with Docker
```bash
docker run -v /path/to/your/csvfiles:/data data-generator:latest \
  --bootstrap-servers <kafka-bootstrap-servers> \
  --topic <kafka-topic> \
  --input-file /data/yourfile.csv \
  --pause-ms <pause-between-records-ms> \
  --num-records <number-of-records-to-process>
```

### Example

```bash
# Process 1000 records from users.csv with a 100ms pause between records
java -jar target/datagenerator.jar \
  --bootstrap-servers localhost:9092 \
  --topic user-events \
  --input-file /data/users.csv \
  --pause-ms 100 \
  --num-records 1000
```

## Configuration Options

| Parameter | Description | Default | Required |
|-----------|-------------|---------|----------|
| --bootstrap-servers | Kafka broker(s) addresses (comma-separated) | None | Yes |
| --topic | Kafka topic to publish to | None | Yes |
| --input-file | Path to the CSV file | None | Yes |
| --pause-ms | Pause between record publishing (milliseconds) | 0 | No |
| --num-records | Number of records to process (-1 for all) | -1 | No |

## Example CSV Format

The CSV file should have headers in the first row. Each subsequent row will be converted to a JSON object where the keys are the header names and the values are from the row.

Example CSV:

```
id,name,email,age
1,John Doe,john@example.com,32
2,Jane Smith,jane@example.com,28
3,Bob Johnson,bob@example.com,45
```

Will be converted to JSON objects like:

```json
{"id":"1","name":"John Doe","email":"john@example.com","age":"32"}
{"id":"2","name":"Jane Smith","email":"jane@example.com","age":"28"}
{"id":"3","name":"Bob Johnson","email":"bob@example.com","age":"45"}
```

## Troubleshooting

### Common Issues

1. **Connection refused to Kafka**
   - Check if the Kafka broker is running
   - Verify the bootstrap servers address
   - Ensure network connectivity (especially in containerized environments)

2. **CSV parsing errors**
   - Check if the CSV file is properly formatted
   - Ensure the file is not corrupted
   - Verify the file encoding (UTF-8 is recommended)

3. **Out of memory errors**
   - Increase the JVM heap size: `java -Xmx2g -jar data-generator.jar ...`

### Logging

The application uses SLF4J with Logback for logging. By default, logs are printed to the console. You can modify the logging behavior by providing a custom `logback.xml` file in the classpath.

## Best Practices for Production

1. **Resource Allocation**
   - Ensure sufficient memory for the JVM (especially for large CSV files)
   - Consider CPU requirements when setting a low pause time between records

2. **Monitoring**
   - Monitor Kafka broker health and topic lag
   - Set up alerts for application failures
   - Keep track of processing rates and adjust configurations accordingly

3. **Reliability**
   - Use a dedicated Kafka topic for production data
   - Consider implementing a dead-letter queue for failed messages
   - Regularly back up your CSV files

4. **Security**
   - Use TLS for Kafka connections in production
   - Implement appropriate authentication mechanisms
   - Secure sensitive data in CSV files

5. **Performance Tuning**
   - Adjust the pause time based on your Kafka cluster capacity
   - Consider batch processing for high-throughput scenarios
   - Monitor and optimize JVM garbage collection

6. **Container Deployment**
   - Set appropriate resource limits for containers
   - Use container orchestration (Kubernetes, Docker Swarm) for high availability
   - Implement proper volume mounting for persistent data

