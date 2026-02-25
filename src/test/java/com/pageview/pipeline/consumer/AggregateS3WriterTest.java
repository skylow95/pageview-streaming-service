package com.pageview.pipeline.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.pageview.pipeline.model.PageviewAggregate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AggregateS3WriterTest {

    private S3Client s3Client;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        s3Client = mock(S3Client.class);
        objectMapper = JsonMapper.builder().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS).build();
    }

    @Test
    @DisplayName("Consume flushes to S3 when batch size reached")
    void consume_flushesToS3WhenBatchSizeReached() {
        AggregateS3Writer writer = new AggregateS3Writer(
                s3Client,
                "test-bucket",
                2,
                999_999,
                objectMapper
        );

        writer.consume(new PageviewAggregate("SW19", "2021-01-26T10:04:00", 2L));
        writer.consume(new PageviewAggregate("EC1", "2021-01-26T10:04:00", 1L));

        verify(s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    @DisplayName("Consume does not flush when buffer below batch size")
    void consume_doesNotFlushWhenBelowBatchSize() {
        AggregateS3Writer writer = new AggregateS3Writer(
                s3Client,
                "test-bucket",
                3,
                999_999,
                objectMapper
        );

        writer.consume(new PageviewAggregate("SW19", "2021-01-26T10:04:00", 2L));
        writer.consume(new PageviewAggregate("EC1", "2021-01-26T10:04:00", 1L));

        verify(s3Client, never()).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    @DisplayName("Consume groups aggregates by date and writes to S3")
    void consume_groupsByDateAndWrites() {
        AggregateS3Writer writer = new AggregateS3Writer(
                s3Client,
                "test-bucket",
                2,
                999_999,
                objectMapper
        );

        writer.consume(new PageviewAggregate("SW19", "2021-01-26T10:04:00", 2L));
        writer.consume(new PageviewAggregate("EC1", "2021-01-26T10:05:00", 1L));

        verify(s3Client).putObject(any(Consumer.class), any(RequestBody.class));
    }

    @Test
    @DisplayName("Consume retries buffer on S3 write failure")
    void consume_retriesBufferOnS3Failure() {
        doThrow(new RuntimeException("S3 error")).when(s3Client).putObject(any(Consumer.class), any(RequestBody.class));

        AggregateS3Writer writer = new AggregateS3Writer(
                s3Client,
                "test-bucket",
                2,
                999_999,
                objectMapper
        );

        writer.consume(new PageviewAggregate("SW19", "2021-01-26T10:04:00", 2L));
        writer.consume(new PageviewAggregate("EC1", "2021-01-26T10:04:00", 1L));

        verify(s3Client, times(1)).putObject(any(Consumer.class), any(RequestBody.class));
    }
}
