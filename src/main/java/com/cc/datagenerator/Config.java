package com.cc.datagenerator;

import lombok.Data;

/**
 * Configuration class for the data generator application.
 * Uses Lombok @Data to automatically generate getters, setters, equals, hashCode, and toString methods.
 */
@Data
public class Config {
    /**
     * Comma-separated list of Kafka bootstrap servers (host:port)
     */
    private String bootstrapServers;
    
    /**
     * Kafka topic to publish messages to
     */
    private String topic;
    
    /**
     * Path to the CSV input file
     */
    private String inputFile;
    
    /**
     * Pause duration in milliseconds between publishing records
     */
    private int pauseMillis;
    
    /**
     * Number of records to process. Use -1 to process all records.
     */
    private int numRecords;
}

