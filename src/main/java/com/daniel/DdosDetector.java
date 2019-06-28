package com.daniel;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


/**
 * <p>Detects DDoS attacks by processing log messages from a kafka cluster.</p>
 */
public class DdosDetector {
    private static final Logger logger = LoggerFactory.getLogger(DdosDetector.class);

    public static final String APACHE_LOG_TOPIC = "apache-log";

    public static void main(String[] args) throws Exception {
        DdosDetector detector = new DdosDetector();
        detector.recreateTopic();
        Path inputFile = Paths.get("src/main/resources/log/apache-access-log.txt.gz");
        detector.publishLog(inputFile);
        Path outputFile = Paths.get("/home/daniel/ddos-result/bot-ips.txt");
        Files.deleteIfExists((outputFile));
        detector.analyzeLog(2, outputFile);
    }

    private final String kafkaHost = "localhost:9092";

    private void recreateTopic() {
        ImmutableMap<String, Object> adminConfig = ImmutableMap.<String, Object>builder()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
                .put(ProducerConfig.ACKS_CONFIG, "all")
                .put(ProducerConfig.RETRIES_CONFIG, 1)
                .build();
        logger.info("Recreating topic: {}", APACHE_LOG_TOPIC);
        try (AdminClient client = KafkaAdminClient.create(adminConfig)) {
            client.deleteTopics(Collections.singleton(APACHE_LOG_TOPIC));
            NewTopic topic = new NewTopic(APACHE_LOG_TOPIC, 1, (short) 1);
            client.createTopics(Collections.singleton(topic));
        }
    }

    /**
     * <p>Publishes the log file to the kafka cluster.</p>
     *
     * @param file the log file to publish
     * @throws IOException if there is a problem reading or publishing the log file
     */
    public void publishLog(Path file) throws IOException {
        requireNonNull(file);
        ImmutableMap<String, Object> publisherConfig = ImmutableMap.<String, Object>builder()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
                .put(ProducerConfig.ACKS_CONFIG, "all")
                .put(ProducerConfig.RETRIES_CONFIG, 1)
                .put(ProducerConfig.BATCH_SIZE_CONFIG, 16384)
                .put(ProducerConfig.LINGER_MS_CONFIG, 1)
                .put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432)
                .put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
                .build();
        try {
            LogPublisher publisher = new LogPublisher(publisherConfig);
            publisher.process(new GzipReader(file), APACHE_LOG_TOPIC);
        } catch (IOException e) {
            throw new IOException("Failed to publish logs", e);
        }
    }


    private static final String IP_REGEX = "\\b(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})\\b";
    private static final Pattern IP_PATTERN = Pattern.compile(IP_REGEX);

    /**
     * <p>Processes the log messages in the kafka cluster and writes suspected bot IP addresses to the given file.</p>
     *
     * <p>The sensitivity is multiplied by the
     * standard deviation to calculate a threshold. Any IP addresses with the total number of requests above that
     * threshold will be considered bots.</p>
     *
     * @param sensitivity the standard deviation multiplier, must be greater than 1
     * @param outputFile  the output file where suspected bot IP addresses will be written, must not exist
     * @throws IOException if outputFile already exists, or if it cannot be created
     */
    public void analyzeLog(double sensitivity, Path outputFile) throws IOException {
        checkArgument(sensitivity > 1, "sensitivity must be greater than 1");
        requireNonNull(outputFile);
        if (Files.exists(outputFile)) {
            throw new FileAlreadyExistsException("Output file already exists: " + outputFile);
        }

        Files.createDirectories(outputFile.getParent());

        Map<String, Integer> ipToCount = aggregateIpAddresses();

        double threshold = calculateBotThreshold(sensitivity, ipToCount);
        persistBotIps(threshold, ipToCount, outputFile);
    }

    private double calculateBotThreshold(double sensitivity, Map<String, Integer> ipToCount) {
        IntSummaryStatistics stats = ipToCount.values().stream()
                .mapToInt(Integer::intValue)
                .summaryStatistics();
        long count = stats.getCount();
        long sum = stats.getSum();
        double avg = stats.getAverage();
        int max = stats.getMax();

        double variance = ipToCount.values().stream()
                .mapToInt(Integer::intValue)
                .mapToDouble(v -> v - avg)
                .map(v -> Math.pow(v, 2))
                .average().orElse(0d);

        double stdDev = Math.sqrt(variance);

        logger.info("Total log messages processed: {}", sum);
        logger.info("Unique IP addresses found: {}", count);
        logger.info("Average requests per IP: {}", avg);
        logger.info("Max requests for an IP: {}", max);
        logger.info("Variance in requests per IP: {}", variance);
        logger.info("Standard deviation in requests per IP: {}", stdDev);
        double threshold = stdDev * sensitivity;
        logger.info("Bot detection request threshold: {}", threshold);
        return threshold;
    }

    private void persistBotIps(double threshold, Map<String, Integer> ipToCount, Path outputFile) throws IOException {
        AtomicInteger suspectedBotnetCount = new AtomicInteger();

        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(outputFile))) {
            ipToCount.entrySet().stream()
                    .filter(entry -> entry.getValue() > threshold)
                    .map(Map.Entry::getKey)
                    .peek(ip -> suspectedBotnetCount.incrementAndGet())
                    .forEach(writer::println);
            logger.info("Wrote suspected botnet IP addresses to file: {}", outputFile);
        } catch (IOException e) {
            throw new IOException("Failed to write results to file: " + outputFile, e);
        }
        logger.info("Suspected bots in botnet: {}", suspectedBotnetCount);
    }

    private Map<String, Integer> aggregateIpAddresses() {
        ImmutableMap<String, Object> consumerConfig = ImmutableMap.<String, Object>builder()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
                .put(ConsumerConfig.GROUP_ID_CONFIG, "apache-log-consumer")
                .put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .build();

        Map<String, Integer> ipToCount = new HashMap<>();

        try (LogConsumer consumer = new LogConsumer(consumerConfig, APACHE_LOG_TOPIC)) {
            int validIpsFound = 0;
            while (consumer.hasNext()) {
                Optional<String> optionalAddress = findIpAddress(ipToCount, consumer, validIpsFound);
                if (optionalAddress.isPresent()) {
                    ipToCount.merge(optionalAddress.get(), 1, Integer::sum);
                    validIpsFound++;
                }
            }
            logger.info("Messages containing a valid IP address: {}", validIpsFound);
        }
        return ipToCount;
    }

    private Optional<String> findIpAddress(Map<String, Integer> ipToCount, LogConsumer consumer, int validIpsFound) {
        ConsumerRecord<String, String> record = consumer.next();
        String line = record.value();
        Matcher matcher = IP_PATTERN.matcher(line);
        while (matcher.find()) {
            // use first valid IP address
            String potentialAddress = matcher.group(0);
            try {
                InetAddress.getByName(potentialAddress);
                return Optional.of(potentialAddress);
            } catch (UnknownHostException e) {
                // the regex isn't exact, so some matches may fail
                logger.warn("Potential IP address invalid: {}", potentialAddress);
            }
        }
        return Optional.empty();
    }
}
