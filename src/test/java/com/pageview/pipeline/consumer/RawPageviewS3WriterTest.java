package com.pageview.pipeline.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.pageview.pipeline.model.Pageview;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RawPageviewS3WriterTest {

    private S3Client s3Client;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        s3Client = mock(S3Client.class);
        objectMapper = JsonMapper.builder().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).build();
    }

    @Test
    @DisplayName("Consume adds pageviews to buffer and flushes to S3 when batch size reached")
    void consume_flushesToS3WhenBatchSizeReached() {
        RawPageviewS3Writer writer = new RawPageviewS3Writer(
                s3Client,
                "test-bucket",
                objectMapper,
                2,
                999_999
        );

        writer.consume(new Pageview(1, "SW19", "www.a.com", 1611662684L));
        writer.consume(new Pageview(2, "EC1", "www.b.com", 1611662685L));

        verify(s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    @DisplayName("Consume does not flush when buffer below batch size")
    void consume_doesNotFlushWhenBelowBatchSize() {
        RawPageviewS3Writer writer = new RawPageviewS3Writer(
                s3Client,
                "test-bucket",
                objectMapper,
                3,
                999_999
        );

        writer.consume(new Pageview(1, "SW19", "www.a.com", 1611662684L));
        writer.consume(new Pageview(2, "EC1", "www.b.com", 1611662685L));

        verify(s3Client, never()).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    @DisplayName("Consume retries buffer on S3 write failure")
    void consume_retriesBufferOnS3Failure() {
        doThrow(new RuntimeException("S3 error")).when(s3Client).putObject(any(Consumer.class), any(RequestBody.class));

        RawPageviewS3Writer writer = new RawPageviewS3Writer(
                s3Client,
                "test-bucket",
                objectMapper,
                2,
                999_999
        );

        writer.consume(new Pageview(1, "SW19", "www.a.com", 1611662684L));
        writer.consume(new Pageview(2, "EC1", "www.b.com", 1611662685L));
        verify(s3Client, atLeast(1)).putObject(any(Consumer.class), any(RequestBody.class));

        writer.consume(new Pageview(3, "W1", "www.c.com", 1611662686L));
        writer.consume(new Pageview(4, "W1", "www.d.com", 1611662687L));
        verify(s3Client, atLeast(2)).putObject(any(Consumer.class), any(RequestBody.class));
    }
}
