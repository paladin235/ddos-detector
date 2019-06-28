package com.daniel;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static java.util.Objects.requireNonNull;

/**
 * A lazy gzip reader.
 *
 * @see Reader
 */
@NotThreadSafe
public class GzipReader implements Reader {
    private final Path file;
    private BufferedReader reader;

    public GzipReader(Path file) {
        this.file = requireNonNull(file);
    }

    @Override
    public Stream<String> read() throws IOException {
        try {
            GZIPInputStream gzipInputStream = new GZIPInputStream(Files.newInputStream(file));
            reader = new BufferedReader(new InputStreamReader(gzipInputStream));
            return reader.lines();
        } catch (IOException e) {
            throw new IOException("Failed to read file: " + file, e);
        }
    }

    @Override
    public String source() {
        return file.toUri().toString();
    }

    @Override
    public void close() throws Exception {
        if (reader != null) {
            reader.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Path logFile = Paths.get("src/main/resources/log/apache-access-log.txt.gz");
        try (GzipReader reader = new GzipReader(logFile)) {
            Stream<String> lines = reader.read();
            System.out.println(lines.count());
        }
    }
}
