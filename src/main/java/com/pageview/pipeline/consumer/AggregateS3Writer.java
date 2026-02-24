package com.pageview.pipeline.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pageview.pipeline.model.PageviewAggregate;
import com.pageview.pipeline.utils.AggregatePathUtils;
import com.pageview.pipeline.utils.BufferUtils;
import com.pageview.pipeline.utils.S3NdjsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class AggregateS3Writer {

    private static final Logger log = LoggerFactory.getLogger(AggregateS3Writer.class);

    private static final String AGGREGATES_PREFIX = "aggregates/";

    private final S3Client s3Client;
    private final String bucketName;
    private final ObjectMapper objectMapper;
    private final int batchSize;

    private final ConcurrentLinkedQueue<PageviewAggregate> buffer = new ConcurrentLinkedQueue<>();

    public AggregateS3Writer(S3Client s3Client,
                             @Value("${aws.s3.bucket-name:pageview-data}") String bucketName,
                             @Value("${pipeline.aggregate.batch-size:50}") int batchSize,
                             @Value("${pipeline.aggregate.flush-interval-seconds:60}") int flushIntervalSeconds,
                             @Qualifier("jsonObjectMapper") ObjectMapper objectMapper) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.batchSize = batchSize;
        this.objectMapper = objectMapper;
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::flush, flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
    }

    @KafkaListener(topics = "${pipeline.aggregates-topic:pageview-aggregates}", groupId = "aggregates-s3-writer", containerFactory = "aggregateListenerFactory")
    public void consume(PageviewAggregate aggregate) {
        buffer.add(aggregate);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    private void flush() {
        List<PageviewAggregate> batch;
        synchronized (buffer) {
            batch = BufferUtils.drain(buffer, batchSize * 2);
        }
        if (batch.isEmpty()) {
            return;
        }

        try {
            Map<String, List<PageviewAggregate>> byDate = batch.stream()
                    .collect(Collectors.groupingBy(agg -> AggregatePathUtils.pathFromWindowStart(agg.windowStart())));
            long timestamp = System.currentTimeMillis();
            for (Map.Entry<String, List<PageviewAggregate>> data : byDate.entrySet()) {
                String key = formatKey(data, timestamp);
                String ndjson = S3NdjsonUtils.toNdjson(data.getValue(), objectMapper);
                S3NdjsonUtils.writeToS3(s3Client, bucketName, key, ndjson);
                log.debug("Wrote {} aggregates to s3://{}/{}", data.getValue().size(), bucketName, key);
            }
        } catch (Exception ex) {
            log.error("Failed to write aggregates to S3", ex);
            buffer.addAll(batch);
        }
    }

    private String formatKey(Map.Entry<String, List<PageviewAggregate>> data, long timestamp) {
        return AGGREGATES_PREFIX + data.getKey() + "/aggregates-" + timestamp + ".ndjson";
    }
}
