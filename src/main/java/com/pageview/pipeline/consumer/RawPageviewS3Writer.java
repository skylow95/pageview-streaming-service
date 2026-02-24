package com.pageview.pipeline.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pageview.pipeline.model.Pageview;
import com.pageview.pipeline.utils.BufferUtils;
import com.pageview.pipeline.utils.S3NdjsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class RawPageviewS3Writer {

    private static final Logger log = LoggerFactory.getLogger(RawPageviewS3Writer.class);

    private static final String PARTITION_PATTERN = "yyyy/MM/dd/HH";
    private static final String RAW_PREFIX = "raw/";

    private final S3Client s3Client;
    private final String bucketName;
    private final ObjectMapper objectMapper;
    private final int batchSize;

    private final ConcurrentLinkedQueue<Pageview> buffer = new ConcurrentLinkedQueue<>();
    private final DateTimeFormatter pathFormatter = DateTimeFormatter.ofPattern(PARTITION_PATTERN).withZone(ZoneOffset.UTC);

    public RawPageviewS3Writer(S3Client s3Client,
                               @Value("${aws.s3.bucket-name:pageview-data}") String bucketName,
                               @Qualifier("jsonObjectMapper") ObjectMapper objectMapper,
                               @Value("${pipeline.raw.batch-size:25}") int batchSize,
                               @Value("${pipeline.raw.flush-interval-seconds:30}") int flushIntervalSeconds) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.objectMapper = objectMapper;
        this.batchSize = batchSize;
        scheduleAndFlush(flushIntervalSeconds);
    }

    private void scheduleAndFlush(int flushIntervalSeconds) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::flushIfNeeded, flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
    }

    @KafkaListener(topics = "${pipeline.pageviews-topic:pageviews}", groupId = "raw-s3-writer", containerFactory = "pageviewListenerFactory")
    public void consume(Pageview pageview) {
        buffer.add(pageview);
        if (buffer.size() >= batchSize) {
            flushIfNeeded();
        }
    }

    private void flushIfNeeded() {
        List<Pageview> batch;
        synchronized (buffer) {
            batch = BufferUtils.drain(buffer, batchSize * 2);
        }
        if (batch.isEmpty()) {
            return;
        }

        String s3Key = buildS3Key();
        try {
            String ndjson = S3NdjsonUtils.toNdjson(batch, objectMapper);
            S3NdjsonUtils.writeToS3(s3Client, bucketName, s3Key, ndjson);
            log.debug("Wrote {} raw pageviews to s3://{}/{}", batch.size(), bucketName, s3Key);
        } catch (Exception e) {
            log.error("Failed to write raw pageviews to S3", e);
            buffer.addAll(batch);
        }
    }

    private String buildS3Key() {
        Instant now = Instant.now();
        String partition = pathFormatter.format(now);
        return RAW_PREFIX + partition + "/pageviews-" + now.toEpochMilli() + ".ndjson";
    }
}
