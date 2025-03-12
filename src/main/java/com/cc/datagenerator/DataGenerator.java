package com.cc.datagenerator;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DataGenerator is the main class for the CSV to Kafka data generator application.
 * This application reads data from a CSV file and publishes each record as a JSON message to Kafka.
 * <p>
 * Main functionalities include:
 * - Reading records from CSV files
 * - Converting records to JSON format
 * - Publishing JSON messages to Kafka
 * - Controlling the rate of message production with configurable pauses
 * - Processing a specific number of records or the entire file
 * - Handling file rewinding when needed
 */
@Slf4j
public class DataGenerator {

    private static final String BOOTSTRAP_SERVERS = "bootstrap-servers";
    private static final String TOPIC = "topic";
    private static final String INPUT_FILE = "input-file";
    private static final String PAUSE_MS = "pause-ms";
    private static final String NUM_RECORDS = "num-records";

    AtomicInteger processedCount = new AtomicInteger(0);
    private String topic;
    private int pauseMs;
    private int numRecords;
    boolean processAll;
    /**
     * Application entry point. Parses command line arguments and starts the data generation process.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        Config config = parseArguments(args);
        if (config == null) {
            return;
        }
        DataGenerator dataGenerator = new DataGenerator();
        dataGenerator.process(config);
    }

    private void process(Config config) {
        log.info("Starting data generator with configuration: {}", config);

        topic = config.getTopic();
        pauseMs = config.getPauseMillis();
        numRecords = config.getNumRecords();
        processAll = numRecords == -1;
        try (CSVReader csvReader = new CSVReader(config.getInputFile());
             KafkaProducerWrapper producer = new KafkaProducerWrapper(config.getBootstrapServers())) {

            processRecords(csvReader, producer, config);

        } catch (Exception e) {
            log.error("Error in data generator: {}", e.getMessage(), e);
            System.exit(1);
        }

        log.info("Data generator completed successfully");
    }

    /**
     * Parses and validates command line arguments.
     *
     * @param args Command line arguments
     * @return Config object with parsed values or null if parsing failed
     */
    private static Config parseArguments(String[] args) {
        Options options = new Options();

        options.addOption(Option.builder()
                .longOpt(BOOTSTRAP_SERVERS)
                .desc("Kafka bootstrap servers (comma-separated list)")
                .hasArg(true)
                .required(true)
                .build());

        options.addOption(Option.builder()
                .longOpt(TOPIC)
                .desc("Kafka topic name")
                .hasArg(true)
                .required(true)
                .build());

        options.addOption(Option.builder()
                .longOpt(INPUT_FILE)
                .desc("Input CSV file path")
                .hasArg(true)
                .required(true)
                .build());

        options.addOption(Option.builder()
                .longOpt(PAUSE_MS)
                .desc("Pause between records in milliseconds")
                .hasArg(true)
                .required(false)
                .type(Number.class)
                .build());

        options.addOption(Option.builder()
                .longOpt(NUM_RECORDS)
                .desc("Number of records to process (-1 for all records)")
                .hasArg(true)
                .required(false)
                .type(Number.class)
                .build());

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("Error parsing command line arguments: {}", e.getMessage());
            formatter.printHelp("java -jar data-generator.jar", options, true);
            return null;
        }

        Config config = new Config();
        config.setBootstrapServers(cmd.getOptionValue(BOOTSTRAP_SERVERS));
        config.setTopic(cmd.getOptionValue(TOPIC));
        config.setInputFile(cmd.getOptionValue(INPUT_FILE));
        
        // Set default values and parse numeric options
        config.setPauseMillis(Integer.parseInt(cmd.getOptionValue(PAUSE_MS, "0")));
        config.setNumRecords(Integer.parseInt(cmd.getOptionValue(NUM_RECORDS, "-1")));

        // Validate configuration
        if (!validateConfig(config)) {
            formatter.printHelp("java -jar data-generator.jar", options, true);
            return null;
        }

        return config;
    }

    /**
     * Validates the provided configuration.
     *
     * @param config Configuration to validate
     * @return true if configuration is valid, false otherwise
     */
    private static boolean validateConfig(Config config) {
        // Validate bootstrap servers
        if (config.getBootstrapServers() == null || config.getBootstrapServers().trim().isEmpty()) {
            log.error("Bootstrap servers must be specified");
            return false;
        }

        // Validate topic
        if (config.getTopic() == null || config.getTopic().trim().isEmpty()) {
            log.error("Topic must be specified");
            return false;
        }

        // Validate input file
        File inputFile = new File(config.getInputFile());
        if (!inputFile.exists() || !inputFile.isFile() || !inputFile.canRead()) {
            log.error("Input file '{}' does not exist, is not a file, or cannot be read", config.getInputFile());
            return false;
        }

        // Validate pause milliseconds
        if (config.getPauseMillis() < 0) {
            log.error("Pause milliseconds must be non-negative");
            return false;
        }

        // num-records can be -1 (all records) or a positive number
        if (config.getNumRecords() < -1 || config.getNumRecords() == 0) {
            log.error("Number of records must be -1 (all records) or a positive number");
            return false;
        }

        return true;
    }

    /**
     * Processes records from the CSV file and publishes them to Kafka.
     *
     * @param csvReader CSV reader instance
     * @param producer Kafka producer instance
     * @param config Application configuration
     * @throws Exception if an error occurs during processing
     */
    private  void processRecords(CSVReader csvReader, KafkaProducerWrapper producer, Config config) throws Exception {


        log.info("Starting to process {} records ({}ms pause between records)",
                processAll ? "all" : numRecords, pauseMs);

        // Main processing loop
        while (processAll || processedCount.get() < numRecords) {
            String json = csvReader.readNextRecordAsJson();
            
            // If end of file is reached, check if we need to continue
            if (json == null) {
                if (processAll) {
                    // If processing all records, we're done
                    break;
                } else if (processedCount.get() < numRecords) {
                    // Rewind the file if we need more records
                    log.info("End of file reached. Rewinding to continue processing.");
                    csvReader.rewind();
                    continue;
                } else {
                    // We've processed the requested number of records
                    break;
                }
            }

            // Publish to Kafka
            producer.sendMessage(topic, json);
            
            // Increment counter and log progress periodically
            int count = processedCount.incrementAndGet();
            if (count % 1000 == 0) {
                log.info("Processed {} records", count);
            }

            // Pause if required
            if (pauseMs > 0) {
                Thread.sleep(pauseMs);
            }
        }

        // Ensure all messages are sent
        producer.flush();
        log.info("Completed processing {} records", processedCount.get());
    }
}

