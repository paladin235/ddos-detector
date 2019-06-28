package com.daniel;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * <p>Publishes logs from a {@link Reader} to a kafka cluster. May be used concurrently with distinct readers.</p>
 */
@ThreadSafe
public class LogPublisher {
    private static final Logger logger = LoggerFactory.getLogger(LogPublisher.class);

    private final Map<String, Object> kafkaConfig;

    private AtomicInteger logMessagesPublished = new AtomicInteger();
    private AtomicInteger logMessagesFailed = new AtomicInteger();

    public LogPublisher(Map<String, Object> kafkaConfig) {
        requireNonNull(kafkaConfig);
        this.kafkaConfig = ImmutableMap.copyOf(kafkaConfig);
    }

    public void process(Reader reader, String topic) throws IOException {
        requireNonNull(reader);
        requireNonNull(topic);
        checkArgument(!topic.isEmpty(), "topic must not be empty");

        logger.info("Publishing logs from {}", reader);

        int processors = Runtime.getRuntime().availableProcessors();
        Semaphore semaphore = new Semaphore(processors * 8);
        try (Producer<String, String> producer = createProducer()) {
            String key = reader.source();
            Stream<String> lines = reader.read();
            lines.parallel()
                    .map(line -> new ProducerRecord<>(topic, key, line))
                    .forEach(record -> {
                        try {
                            semaphore.acquire();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            logger.error("Interrupted while trying to acquire semaphore");
                            return;
                        }
                        producer.send(record, (recordMetadata, e) -> onCompletion(recordMetadata, e, semaphore));
                    });
            try {
                semaphore.acquire(processors);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for messages to be sent");
            }
        } catch (IOException e) {
            throw new IOException("Failed to process reader content", e);
        } finally {
            try {
                reader.close();
            } catch (Exception e) {
                logger.warn("Failed to close reader: {}", reader, e);
            }

        }
        logger.info("Log messages published: {}", logMessagesPublished.get());
        logger.info("Log messages failed: {}", logMessagesFailed.get());
    }

    private void onCompletion(RecordMetadata recordMetadata, Exception e, Semaphore semaphore) {
        try {
            if (e != null) {
                logger.warn("Failed to send record: {}", recordMetadata, e);
                logMessagesFailed.incrementAndGet();
            } else {
                int count = logMessagesPublished.incrementAndGet();
                if (count % 10000 == 0) {
                    logger.info("Published {} log messages", count);
                }
            }
        } finally {
            semaphore.release();
        }
    }

    private Producer<String, String> createProducer() {
        return new KafkaProducer<>(kafkaConfig);
    }
}
