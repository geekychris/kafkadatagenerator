package com.cc.datagenerator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

/**
 * CSVReader class handles reading and parsing CSV files.
 * It provides functionality to read headers and records from a CSV file,
 * convert records to JSON format, and rewind the file when needed.
 * This class implements AutoCloseable for proper resource management.
 */
public class CSVReader implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CSVReader.class);
    
    private final String filePath;
    private BufferedReader reader;
    private CSVParser csvParser;
    private String[] headers;
    private Iterator<CSVRecord> recordIterator;
    private final ObjectMapper objectMapper;
    
    /**
     * Constructs a CSVReader with the given file path.
     * 
     * @param filePath the path to the CSV file to be read
     * @throws IOException if an I/O error occurs opening the file
     */
    public CSVReader(String filePath) throws IOException {
        this.filePath = filePath;
        this.objectMapper = new ObjectMapper();
        
        Path path = Paths.get(filePath);
        if (!Files.exists(path)) {
            throw new IOException("File does not exist: " + filePath);
        }
        
        initializeReader();
    }
    
    /**
     * Initializes the CSV reader, parser, and reads the headers.
     * 
     * @throws IOException if an I/O error occurs during initialization
     */
    private void initializeReader() throws IOException {
        logger.debug("Initializing CSV reader for file: {}", filePath);
        reader = new BufferedReader(new FileReader(filePath));
        csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
        headers = csvParser.getHeaderNames().toArray(new String[0]);
        recordIterator = csvParser.iterator();
        
        logger.debug("CSV headers: {}", String.join(", ", headers));
    }
    
    /**
     * Gets the headers of the CSV file.
     * 
     * @return an array of header names
     */
    public String[] getHeaders() {
        return headers;
    }
    
    /**
     * Checks if there are more records available to read.
     * 
     * @return true if more records exist, false otherwise
     */
    public boolean hasNext() {
        return recordIterator.hasNext();
    }
    
    /**
     * Reads the next record from the CSV file and converts it to JSON.
     * 
     * @return a JSON string representation of the record or null if no more records
     * @throws IOException if an I/O error occurs while reading
     */
    public String readNextRecordAsJson() throws IOException {
        if (!hasNext()) {
            logger.debug("No more records to read");
            return null;
        }
        
        try {
            CSVRecord record = recordIterator.next();
            ObjectNode jsonNode = objectMapper.createObjectNode();
            
            for (String header : headers) {
                String value = record.get(header);
                jsonNode.put(header, value);
            }
            
            String json = objectMapper.writeValueAsString(jsonNode);
            logger.debug("Converted record to JSON: {}", json);
            return json;
        } catch (Exception e) {
            logger.error("Error reading record: {}", e.getMessage(), e);
            throw new IOException("Error reading CSV record", e);
        }
    }
    
    /**
     * Rewinds the file to begin reading from the start again.
     * This method closes the current reader and parser, then reinitializes them.
     * 
     * @throws IOException if an I/O error occurs during the rewind operation
     */
    public void rewind() throws IOException {
        logger.info("Rewinding CSV file: {}", filePath);
        try {
            close();
        } catch (Exception e) {
            logger.warn("Error while closing resources during rewind: {}", e.getMessage());
        }
        
        initializeReader();
    }
    
    /**
     * Reads all records from the CSV file as a list of JSON strings.
     * This method is memory-intensive for large files.
     * 
     * @return a list of JSON strings, each representing a record
     * @throws IOException if an I/O error occurs while reading
     */
    public List<String> readAllRecordsAsJson() throws IOException {
        logger.info("Reading all records from CSV file: {}", filePath);
        rewind(); // Make sure we're starting from the beginning
        
        List<String> jsonRecords = new java.util.ArrayList<>();
        String record;
        while ((record = readNextRecordAsJson()) != null) {
            jsonRecords.add(record);
        }
        
        logger.info("Read {} records from CSV file", jsonRecords.size());
        return jsonRecords;
    }
    
    @Override
    public void close() throws Exception {
        logger.debug("Closing CSV reader resources");
        if (csvParser != null) {
            try {
                csvParser.close();
            } catch (IOException e) {
                logger.warn("Error closing CSV parser: {}", e.getMessage());
            }
        }
        
        if (reader != null) {
            try {
                reader.close();
            } catch (IOException e) {
                logger.warn("Error closing reader: {}", e.getMessage());
            }
        }
    }
}

