package com.daniel;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * A reader for lazily reading things from some source.
 */
public interface Reader extends AutoCloseable {

    /**
     * Lazily reads from the source.
     *
     * @return the lines in the source
     * @throws IOException if there is a problem reading
     */
    Stream<String> read() throws IOException;

    /**
     * Gets the source name.
     *
     * @return the source name
     */
    String source();

    @Override
    void close() throws Exception;
}
