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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


/**
 * <p>Detects DDoS attacks by processing log messages from a kafka cluster.</p>
 *
 * <p>
 * Log messages are processed within a sliding window. The detector waits for new messages to come in and will analyze
 * log messages for bots when either messages outside the sliding window are received, or when a timer elapses equal to
 * the sliding window time in seconds. This allows detection of bots on real-time logs, archived logs, and logs from sources
 * that may not have steady traffic.
 * </p>
 */
@NotThreadSafe
public class DdosDetector {
    private static final Logger logger = LoggerFactory.getLogger(DdosDetector.class);

    public static final String kafkaHost = "localhost:9092";
    public static final String APACHE_LOG_TOPIC = "apache-log";

    private static final String IP_REGEX = "\\b(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})\\b";
    private static final Pattern IP_PATTERN = Pattern.compile(IP_REGEX);

    private static final String DATE_REGEX = "\\[(\\d\\d/[A-Za-z]+?/\\d{4}:\\d\\d:\\d\\d:\\d\\d \\+\\d\\d\\d\\d)\\]";
    private static final Pattern DATE_PATTERN = Pattern.compile(DATE_REGEX);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd/MMMM/yyyy:HH:mm:ss ZZZ");

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected exactly two arguments: command path");
        }

        String command = args[0];
        Path path = Paths.get(args[1]).toAbsolutePath();

        if (command.equals("publish")) {
            createTopic();
            publishLog(path);
        } else if (command.equals("detect")) {
            DdosDetector detector = new DdosDetector(2.0, 60, path);
            detector.analyzeLogs();
        } else {
            throw new IllegalArgumentException("Unrecognized command: " + command);
        }
    }

    private static void createTopic() {
        ImmutableMap<String, Object> adminConfig = ImmutableMap.<String, Object>builder()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
                .put(ProducerConfig.ACKS_CONFIG, "all")
                .put(ProducerConfig.RETRIES_CONFIG, 1)
                .build();
        logger.info("Creating topic: {}", APACHE_LOG_TOPIC);
        try (AdminClient client = KafkaAdminClient.create(adminConfig)) {
            NewTopic topic = new NewTopic(APACHE_LOG_TOPIC, 1, (short) 1);
            client.createTopics(Collections.singleton(topic));
        }
    }

    private static void recreateTopic() {
        ImmutableMap<String, Object> adminConfig = ImmutableMap.<String, Object>builder()
                .put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
                .put(ProducerConfig.ACKS_CONFIG, "all")
                .put(ProducerConfig.RETRIES_CONFIG, 1)
                .build();
        logger.info("Deleting topic: {}", APACHE_LOG_TOPIC);
        try (AdminClient client = KafkaAdminClient.create(adminConfig)) {
            client.deleteTopics(Collections.singleton(APACHE_LOG_TOPIC));
        }
    }

    /**
     * <p>Publishes the log file to the kafka cluster.</p>
     *
     * @param file the log file to publish
     * @throws IOException if there is a problem reading or publishing the log file
     */
    public static void publishLog(Path file) throws IOException {
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

    private final Map<String, Integer> ipToCount = new HashMap<>();
    private final double sensitivity;
    private final int slidingWindowSeconds;
    private final Path outputDirectory;
    private Timer timer;
    private Instant windowStart;

    /**
     * Creates an instance with the provided settings.
     *
     * @param sensitivity          the standard deviation multiplier, must be greater than 1
     * @param slidingWindowSeconds the number of seconds in the sliding window to detect attacks
     * @param outputDirectory      the output directory where suspected bot IP addresses will be written
     */
    public DdosDetector(double sensitivity, int slidingWindowSeconds, Path outputDirectory) {
        this.sensitivity = sensitivity;
        checkArgument(sensitivity > 1, "sensitivity must be greater than 1");
        this.slidingWindowSeconds = slidingWindowSeconds;
        this.outputDirectory = requireNonNull(outputDirectory);
    }

    /**
     * <p>Processes the log messages in the kafka cluster and writes suspected bot IP addresses to the given file.</p>
     *
     * <p>The sensitivity is multiplied by the
     * standard deviation to calculate a threshold. Any IP addresses with the total number of requests above that
     * threshold will be considered bots.</p>
     *
     * @throws IOException if outputDirectory cannot be created or written to
     */
    public void analyzeLogs() throws IOException {
        Files.createDirectories(outputDirectory);

        try (LogConsumer consumer = createConsumer()) {
            while (!consumer.isClosed()) {
                ConsumerRecord<String, String> record = consumer.poll(1000);
                if (record == null) {
                    continue;
                }

                String line = record.value();
                Optional<String> optionalAddress = findIpAddress(line);
                Optional<Instant> optionalTimestamp = findTimestamp(line);
                if (!optionalAddress.isPresent() || !optionalTimestamp.isPresent()) {
                    // IP address or timestamp were not found, continue with next record
                    continue;
                }

                Instant timestamp = optionalTimestamp.get();
                if (windowStart == null) {
                    // started a new window, so the timer should run when the
                    windowStart = timestamp;
                    createTimer();
                }

                long secondsElapsed = Duration.between(windowStart, timestamp).getSeconds();
                if (secondsElapsed > slidingWindowSeconds) {
                    detectBotAddresses();
                } else {
                    ipToCount.merge(optionalAddress.get(), 1, Integer::sum);
                }
            }
        }
    }

    private LogConsumer createConsumer() {
        ImmutableMap<String, Object> consumerConfig = ImmutableMap.<String, Object>builder()
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost)
                .put(ConsumerConfig.GROUP_ID_CONFIG, "apache-log-consumer")
                .put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
                .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                .build();
        return new LogConsumer(consumerConfig, APACHE_LOG_TOPIC);
    }

    private void createTimer() {
        if (timer != null) {
            timer.cancel();
        }

        timer = new Timer(true);
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                detectBotAddresses();
            }
        }, slidingWindowSeconds * 1000, slidingWindowSeconds * 1000);
    }

    private synchronized void detectBotAddresses() {
        if (!ipToCount.isEmpty()) {
            timer.cancel();
            double threshold = calculateBotThreshold(sensitivity, ipToCount);
            try {
                persistBotIps(threshold, ipToCount, outputDirectory, windowStart);
            } catch (IOException e) {
                logger.error("Failed to persist suspected bot IP addresses", e);
            }
            ipToCount.clear();
        }
        windowStart = null;
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

        logger.info("Total log messages in window: {}", sum);
        logger.info("Unique IP addresses found: {}", count);
        logger.info("Average requests per IP: {}", avg);
        logger.info("Max requests for an IP: {}", max);
        logger.info("Variance in requests per IP: {}", variance);
        logger.info("Standard deviation in requests per IP: {}", stdDev);
        double threshold = stdDev * sensitivity;
        logger.info("Bot detection request threshold: {}", threshold);
        return threshold;
    }

    private void persistBotIps(double threshold, Map<String, Integer> ipToCount, Path outputDirectory, Instant windowStart) throws IOException {
        Path outputFile = outputDirectory.resolve((windowStart.toString() + ".txt"));
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

    private Optional<Instant> findTimestamp(String text) {
        Matcher matcher = DATE_PATTERN.matcher(text);
        while (matcher.find()) {
            String rawDate = matcher.group(1);
            try {
                ZonedDateTime dateTime = ZonedDateTime.parse(rawDate, DATE_FORMATTER);
                return Optional.of(dateTime.toInstant());
            } catch (DateTimeParseException e) {
                logger.warn("Raw date could not be parsed: {}", rawDate);
            }
        }
        return Optional.empty();
    }

    private Optional<String> findIpAddress(String text) {
        Matcher matcher = IP_PATTERN.matcher(text);
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
