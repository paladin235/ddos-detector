package com.daniel;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 *
 */
@ThreadSafe
public class LogPublisher {
    private static final Logger logger = LoggerFactory.getLogger(LogPublisher.class);

    private final Map<String, Object> kafkaConfig;

    public LogPublisher(Map<String, Object> kafkaConfig) {
        requireNonNull(kafkaConfig);
        this.kafkaConfig = ImmutableMap.copyOf(kafkaConfig);
    }

    public void process(Reader reader) throws IOException {
        requireNonNull(reader);

        try (Producer<String, String> producer = createProducer()) {
            String key = reader.source();
            Stream<String> lines = reader.read();
            lines.parallel()
                    .map(line -> new ProducerRecord<>("apache-log", key, line))
                    .forEach(record -> producer.send(record, ((recordMetadata, e) -> {
                        if (e != null) {
                            logger.warn("Failed to send record: {}", recordMetadata, e);
                        }
                    })));
        } catch (IOException e) {
            throw new IOException("Failed to process reader content", e);
        } finally {
            try {
                reader.close();
            } catch (Exception e) {
                logger.warn("Failed to close reader: {}", reader, e);
            }

        }
    }

    private Producer<String, String> createProducer() {
        return new KafkaProducer<>(kafkaConfig);
    }
}
