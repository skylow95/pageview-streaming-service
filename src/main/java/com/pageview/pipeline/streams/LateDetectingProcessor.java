package com.pageview.pipeline.streams;

import com.pageview.pipeline.model.Pageview;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Processor that detects late events (arriving after window + grace) and produces them to a
 * separate Kafka topic. On-time events are forwarded for aggregation.
 */
public class LateDetectingProcessor extends ContextualProcessor<String, Pageview, String, Pageview> {

    private final KafkaTemplate<String, Pageview> kafkaTemplate;
    private final String lateTopic;
    private final long windowSizeMs;
    private final long graceMs;

    public LateDetectingProcessor(KafkaTemplate<String, Pageview> kafkaTemplate,
                                  String lateTopic,
                                  long windowSizeMs,
                                  long graceMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.lateTopic = lateTopic;
        this.windowSizeMs = windowSizeMs;
        this.graceMs = graceMs;
    }

    @Override
    public void process(Record<String, Pageview> record) {
        Pageview value = record.value();
        if (value == null) {
            return;
        }
        long recordTsMs = value.timestamp() * 1000L;
        long streamTimeMs = context().currentStreamTimeMs();

        long windowStartMs = (recordTsMs / windowSizeMs) * windowSizeMs;
        long windowEndMs = windowStartMs + windowSizeMs;
        long windowCloseMs = windowEndMs + graceMs;

        // Late: stream time has already passed when this window would close
        if (streamTimeMs >= windowCloseMs) {
            kafkaTemplate.send(lateTopic, value.postcode(), value);
            return;
        }
        context().forward(record);
    }
}
