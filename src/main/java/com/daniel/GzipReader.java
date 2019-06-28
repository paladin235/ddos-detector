package com.daniel;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static java.util.Objects.requireNonNull;

/**
 * <p>A lazy gzip reader. </p>
 * <p>The file may be read multiple times, but each call to {@link #read()} must be followed by a call to {@link #close()} prior to reading again.</p>
 *
 * @see Reader
 */
@NotThreadSafe
public class GzipReader implements Reader {
    private final Path file;
    private BufferedReader reader;

    /**
     * Creates a reader for the given file.
     *
     * @param file the file to read, must not be null
     */
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

    @Override
    public String toString() {
        return "GzipReader{" +
                "file=" + file +
                '}';
    }
}
