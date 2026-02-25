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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class LateAggregateS3WriterTest {

    private S3Client s3Client;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        s3Client = mock(S3Client.class);
        objectMapper = JsonMapper.builder().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).build();
    }

    @Test
    @DisplayName("Consume flushes and flushes late pageviews to S3 when batch size reached")
    void consume_flushesToS3WhenBatchSizeReached() {
        long timestamp = 1611662684;

        LateAggregateS3Writer writer = new LateAggregateS3Writer(
                s3Client,
                "test-bucket",
                2,
                60L,
                999_999,
                objectMapper
        );

        writer.consume(new Pageview(1, "SW19", "www.a.com", timestamp));
        writer.consume(new Pageview(2, "EC1", "www.b.com", timestamp));

        verify(s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    @DisplayName("Consume does not flush when buffer below batch size")
    void consume_doesNotFlushWhenBelowBatchSize() {
        long timestamp = 1611662684;

        LateAggregateS3Writer writer = new LateAggregateS3Writer(
                s3Client,
                "test-bucket",
                3,
                60L,
                999_999,
                objectMapper
        );

        writer.consume(new Pageview(1, "SW19", "www.a.com", timestamp));
        writer.consume(new Pageview(2, "EC1", "www.b.com", timestamp));

        verify(s3Client, never()).putObject(any(Consumer.class), any(RequestBody.class));
    }


    @Test
    @DisplayName("Consume retries buffer on S3 write failure")
    void consume_retriesBufferOnS3Failure() {
        doThrow(new RuntimeException("S3 error")).when(s3Client).putObject(any(Consumer.class), any(RequestBody.class));

        long timestamp = 1611662684;
        LateAggregateS3Writer writer = new LateAggregateS3Writer(
                s3Client,
                "test-bucket",
                2,
                60L,
                999_999,
                objectMapper
        );

        writer.consume(new Pageview(1, "SW19", "www.a.com", timestamp));
        writer.consume(new Pageview(2, "EC1", "www.b.com", timestamp));

        verify(s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
    }
}
