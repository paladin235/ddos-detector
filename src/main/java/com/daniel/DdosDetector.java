package com.daniel;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DdosDetector {
    private static final Logger logger = LoggerFactory.getLogger(DdosDetector.class);

    public static final String APACHE_LOG_TOPIC = "apache-log";

    public static void main(String[] args) throws Exception {
        DdosDetector detector = new DdosDetector();
        detector.publishLog();
    }

    public void publishLog() throws IOException {
        ImmutableMap<String, Object> publisherConfig = ImmutableMap.<String, Object>builder()
                .put("bootstrap.servers", "localhost:9092")
                .put("acks", "all")
                .put("retries", 0)
                .put("batch.size", 16384)
                .put("linger.ms", 1)
                .put("buffer.memory", 33554432)
                .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
                .build();
        try {
            LogPublisher publisher = new LogPublisher(publisherConfig);
            publisher.process(new GzipReader(Paths.get("src/main/resources/log/apache-access-log.txt.gz")), APACHE_LOG_TOPIC);
        } catch (IOException e) {
            throw new IOException("Failed to publish logs", e);
        }
    }


    private static final String IP_REGEX = "\\b(\\d{3}\\.\\d{3}\\.\\d{3}\\.\\d{3})\\b";
    private static final Pattern IP_PATTERN = Pattern.compile(IP_REGEX);

    public void processLog() {
        ImmutableMap<String, Object> consumerConfig = ImmutableMap.<String, Object>builder()
                .put("bootstrap.servers", "localhost:9092")
                .put("group.id", "test")
                .put("enable.auto.commit", "true")
                .put("auto.commit.interval.ms", "1000")
                .put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                .build();

        Map<String, Integer> ipToCount = new HashMap<>();

        try (LogConsumer consumer = new LogConsumer(consumerConfig, APACHE_LOG_TOPIC)) {
            while (consumer.hasNext()) {
                ConsumerRecord<String, String> record = consumer.next();
                String line = record.value();
                Matcher matcher = IP_PATTERN.matcher(line);
                if (matcher.find()) {
                    String potentialAddress = matcher.group(0);
                    try {
                        InetAddress.getByName(potentialAddress);
                        ipToCount.merge(potentialAddress, 1, Integer::sum);
                    } catch (UnknownHostException e) {
                        // the regex isn't exact, so some matches may fail
                    }
                }


            }
        }
    }
}
