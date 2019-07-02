package com.daniel;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * <p>Consumes logs from a kafka topic.</p>
 */
@NotThreadSafe
public class LogConsumer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LogConsumer.class);
    private final ImmutableMap<String, Object> kafkaConfig;
    private final String topic;

    private Consumer<String, String> consumer;
    private Iterator<ConsumerRecord<String, String>> recordIterator = Collections.emptyIterator();
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

    /**
     * Polls for more records, waiting up to the given number of milliseconds if none are immediately available.
     *
     * @param ms the number of milliseconds to wait
     * @return a record if available, null otherwise
     */
    public @Nullable
    ConsumerRecord<String, String> poll(int ms) {
        if (closed) {
            return null;
        }

        if (consumer == null) {
            // first call, create consumer and subscribe
            consumer = createConsumer();
            consumer.subscribe(Collections.singleton(topic));
            logger.info("Subscribed to topic: {}", topic);
        }

        if (!recordIterator.hasNext()) {
            // try to retrieve more records
            recordIterator = consumer.poll(Duration.ofMillis(ms)).iterator();
        }

        if (recordIterator.hasNext()) {
            // found some records!
            ConsumerRecord<String, String> next = recordIterator.next();
            count++;
            return next;
        } else {
            // still no records
            return null;
        }

    }

    /**
     * Returns true if this consumer is closed, false otherwise.
     *
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
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
