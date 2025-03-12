package com.cc.datagenerator;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A wrapper class for Kafka Producer that simplifies configuration and provides 
 * additional error handling and logging.
 * <p>
 * This class implements AutoCloseable for proper resource management and provides
 * methods for sending messages to Kafka topics with configurable reliability settings.
 * </p>
 */
@Slf4j
public class KafkaProducerWrapper implements AutoCloseable {

    private final KafkaProducer<String, String> producer;
    private final int defaultTimeoutMs = 30000; // 30 seconds default timeout
    
    /**
     * Creates a new KafkaProducerWrapper with the specified bootstrap servers.
     *
     * @param bootstrapServers A comma-separated list of host:port pairs for establishing the initial connection to the Kafka cluster
     */
    public KafkaProducerWrapper(String bootstrapServers) {
        log.info("Initializing Kafka producer with bootstrap servers: {}", bootstrapServers);
        this.producer = new KafkaProducer<>(createProducerConfig(bootstrapServers));
    }

    /**
     * Creates the Kafka producer configuration with optimal settings for reliability.
     *
     * @param bootstrapServers A comma-separated list of host:port pairs for the Kafka cluster
     * @return Properties object with Kafka producer configuration
     */
    private Properties createProducerConfig(String bootstrapServers) {
        Properties props = new Properties();
        
        // Basic connection properties
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Strongest durability guarantee
        props.put(ProducerConfig.RETRIES_CONFIG, 3);  // Retry on transient errors
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // Ensure strict ordering with retries
        
        // Performance tuning (balanced for reliability)
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5); // Small delay for batching
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // Default batch size
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB buffer
        
        // Additional settings for better error handling
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // Avoid duplicates
        
        return props;
    }

    /**
     * Sends a message to the specified topic with no key.
     *
     * @param topic The Kafka topic to send the message to
     * @param value The message content
     * @throws KafkaException If there's an error sending the message
     */
    public void sendMessage(String topic, String value) {
        sendMessage(topic, null, value);
    }

    /**
     * Sends a message to the specified topic with the given key.
     *
     * @param topic The Kafka topic to send the message to
     * @param key The message key (can be null)
     * @param value The message content
     * @throws KafkaException If there's an error sending the message
     */
    public void sendMessage(String topic, String key, String value) {
        try {
            log.debug("Sending message to topic {}: key={}, value={}", topic, key, value);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message to topic {}: {}", topic, exception.getMessage(), exception);
                } else {
                    log.debug("Message sent successfully to topic {}, partition {}, offset {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        } catch (ProducerFencedException e) {
            log.error("Producer has been fenced off from the broker", e);
            throw new KafkaException("Producer has been fenced off: " + e.getMessage(), e);
        } catch (KafkaException e) {
            log.error("Error sending message to Kafka", e);
            throw e;
        }
    }

    /**
     * Sends a message to the specified topic and waits for acknowledgement.
     *
     * @param topic The Kafka topic to send the message to
     * @param key The message key (can be null)
     * @param value The message content
     * @return The RecordMetadata for the sent message
     * @throws KafkaException If there's an error sending the message
     * @throws InterruptedException If the thread is interrupted while waiting
     * @throws ExecutionException If there's an error in the message sending execution
     * @throws TimeoutException If the send operation times out
     */
    public RecordMetadata sendMessageSync(String topic, String key, String value) 
            throws InterruptedException, ExecutionException, TimeoutException {
        try {
            log.debug("Sending message synchronously to topic {}: key={}, value={}", topic, key, value);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
            Future<RecordMetadata> future = producer.send(record);
            return future.get(defaultTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (KafkaException e) {
            log.error("Error sending message to Kafka", e);
            throw e;
        }
    }

    /**
     * Flushes any buffered records.
     */
    public void flush() {
        log.debug("Flushing Kafka producer");
        producer.flush();
    }

    /**
     * Closes the producer and releases all resources.
     * This method should be called when the producer is no longer needed.
     */
    @Override
    public void close() {
        if (producer != null) {
            log.info("Closing Kafka producer");
            try {
                producer.flush();
                producer.close();
                log.info("Kafka producer closed successfully");
            } catch (Exception e) {
                log.error("Error closing Kafka producer", e);
            }
        }
    }
}

