package com.daniel;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;
import static java.util.stream.Collectors.toList;

public class GzipReaderTest {

    private static final String SOURCE = "src/test/resources/GzipReader/test.txt.gz";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private GzipReader fixture;

    @Before
    public void setup() {
        fixture = new GzipReader(Paths.get(SOURCE));
    }

    @Test
    public void test_null_path() {
        exception.expect(NullPointerException.class);
        new GzipReader(null);
    }

    @Test
    public void test_read_invalid_path() throws IOException {
        exception.expect(IOException.class);
        fixture = new GzipReader(Paths.get("/some/bogus/path/file.txt"));
        fixture.read();
    }

    @Test
    public void test_read() throws IOException {
        Stream<String> lines = fixture.read();
        List<String> result = lines.collect(toList());
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isEqualTo("hello");
    }

    @Test
    public void test_source() {
        assertThat(fixture.source()).isEqualTo(Paths.get(SOURCE).toUri().toString());
    }

    @Test
    public void test_close_before_read() throws Exception {
        fixture.close();
    }

    @Test
    public void test_close_after_read() throws Exception {
        fixture.read();
        fixture.close();
    }

    @Test
    public void test_close_multiple() throws Exception {
        fixture.close();
        fixture.close();
    }

    @Test
    public void test_toString() {
        assertThat(fixture.toString()).isNotEmpty();
    }
}