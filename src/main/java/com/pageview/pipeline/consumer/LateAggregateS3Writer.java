package com.pageview.pipeline.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pageview.pipeline.model.Pageview;
import com.pageview.pipeline.model.PageviewAggregate;
import com.pageview.pipeline.utils.AggregatePathUtils;
import com.pageview.pipeline.utils.BufferUtils;
import com.pageview.pipeline.utils.S3NdjsonUtils;
import io.micrometer.core.instrument.Metrics;
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
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class LateAggregateS3Writer {

    private static final Logger log = LoggerFactory.getLogger(LateAggregateS3Writer.class);

    private static final DateTimeFormatter WINDOW_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneOffset.UTC);
    private static final String AGGREGATES_PREFIX = "aggregates/";
    private static final String METRIC_S3_WRITES = "s3.writes";
    private static final String METRIC_S3_ERRORS = "s3.errors";
    private static final String TAG_WRITER = "writer";

    private final S3Client s3Client;
    private final String bucketName;
    private final ObjectMapper objectMapper;
    private final long windowSizeSeconds;
    private final int batchSize;

    private final ConcurrentLinkedQueue<PageviewAggregate> buffer = new ConcurrentLinkedQueue<>();

    public LateAggregateS3Writer(S3Client s3Client,
                                @Value("${aws.s3.bucket-name:pageview-data}") String bucketName,
                                @Value("${pipeline.late.batch-size:50}") int batchSize,
                                 @Value("${pipeline.stream.window-size-seconds:60}") long windowSizeSeconds,
                                @Value("${pipeline.late.flush-interval-seconds:60}") int flushIntervalSeconds,
                                @Qualifier("jsonObjectMapper") ObjectMapper objectMapper) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
        this.windowSizeSeconds = windowSizeSeconds;
        this.batchSize = batchSize;
        this.objectMapper = objectMapper;
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(this::flush, flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
    }

    @KafkaListener(topics = "${pipeline.late-topic:late-pageview-aggregates}", groupId = "late-pageviews-s3-writer", containerFactory = "pageviewListenerFactory")
    public void consume(Pageview pageview) {
        long windowStartSec = (pageview.timestamp() / windowSizeSeconds) * windowSizeSeconds;
        String windowStart = WINDOW_FORMATTER.format(Instant.ofEpochSecond(windowStartSec).atZone(ZoneOffset.UTC));
        buffer.add(new PageviewAggregate(pageview.postcode(), windowStart, 1L));

        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    private void flush() {
        List<PageviewAggregate> batch;
        synchronized (buffer) {
            batch = BufferUtils.drain(buffer, batchSize * 2);
        }
        if (batch.isEmpty()) return;

        try {
            Map<String, List<PageviewAggregate>> byDate = batch.stream()
                    .collect(Collectors.groupingBy(agg -> AggregatePathUtils.pathFromWindowStart(agg.windowStart())));
            long timestamp = System.currentTimeMillis();
            int index = 0;
            for (Map.Entry<String, List<PageviewAggregate>> data : byDate.entrySet()) {
                String key = AGGREGATES_PREFIX + data.getKey() + "/late-" + timestamp + "-" + index++ + ".ndjson";
                String ndjson = S3NdjsonUtils.toNdjson(data.getValue(), objectMapper);
                S3NdjsonUtils.writeToS3(s3Client, bucketName, key, ndjson);
                log.debug("Wrote {} late aggregates to s3://{}/{}", data.getValue().size(), bucketName, key);
            }
            Metrics.counter(METRIC_S3_WRITES, TAG_WRITER, "late").increment(batch.size());
        } catch (Exception e) {
            Metrics.counter(METRIC_S3_ERRORS, TAG_WRITER, "late").increment();
            log.error("Failed to write late aggregates to S3", e);
            buffer.addAll(batch);
        }
    }
}
