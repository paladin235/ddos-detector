package com.daniel;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

/**
 * <p>Consumes logs from a kafka topic.</p>
 */
@NotThreadSafe
public class LogConsumer implements Iterator<ConsumerRecord<String, String>>, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LogConsumer.class);
    private final ImmutableMap<String, Object> kafkaConfig;
    private final String topic;

    private Consumer<String, String> consumer;
    private Iterator<ConsumerRecord<String, String>> recordIterator;
    private boolean closed;
    private int count;

    /**
     * Creates a consumer that will use the given kafka config and topic to consume log messages.
     *
     * @param kafkaConfig the kafka config to use to create the consumer
     * @param topic       the topic to subscribe to
     */
    public LogConsumer(Map<String, Object> kafkaConfig, String topic) {
        requireNonNull(kafkaConfig);
        this.kafkaConfig = ImmutableMap.copyOf(kafkaConfig);
        this.topic = requireNonNull(topic);
    }

    private Consumer<String, String> createConsumer() {
        return new KafkaConsumer<>(kafkaConfig);
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            return false;
        }

        if (consumer == null) {
            // first call, create consumer and subscribe
            consumer = createConsumer();
            consumer.subscribe(Collections.singleton(topic));
            logger.info("Subscribed to topic: {}", topic);
        }

        if (recordIterator != null && recordIterator.hasNext()) {
            // continue to use existing iterator
            return true;
        }

        // try to retrieve more records
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        recordIterator = records.iterator();
        boolean next = recordIterator.hasNext();
        if (!next) {
            close();
        }
        return next;
    }

    @Override
    public ConsumerRecord<String, String> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more records for topic: " + topic);
        }
        count++;
        return recordIterator.next();
    }

    @Override
    public void close() {
        closed = true;
        if (consumer != null) {
            consumer.close();
            consumer = null;
            logger.info("Closed after reading {} records", count);
        }
    }
}
