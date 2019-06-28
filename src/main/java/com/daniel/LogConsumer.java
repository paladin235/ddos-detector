package com.daniel;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

@NotThreadSafe
public class LogConsumer implements Iterator<ConsumerRecord<String, String>>, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LogConsumer.class);
    private final ImmutableMap<String, Object> kafkaConfig;
    private final String topic;

    private Consumer<String, String> consumer;
    private Iterator<ConsumerRecord<String, String>> records;

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
        if (consumer == null) {
            // first call, create consumer and subscribe
            consumer = createConsumer();
            consumer.subscribe(Collections.singleton(topic));
        }

        // TODO: fix this
        if (records != null) {
            return records.hasNext();
        }


        records = consumer.poll(Duration.ofMillis(100)).iterator();
        boolean next = records.hasNext();
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
        return records.next();
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }
}
