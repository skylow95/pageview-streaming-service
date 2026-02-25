package com.pageview.pipeline.streams;

import com.pageview.pipeline.model.Pageview;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.kafka.core.KafkaTemplate;

public class LateDetectingTransformer implements Transformer<String, Pageview, KeyValue<String, Pageview>> {

    protected ProcessorContext context;
    private final KafkaTemplate<String, Pageview> kafkaTemplate;
    private final String lateTopic;
    private final long windowSizeMs;
    private final long graceMs;

    public LateDetectingTransformer(KafkaTemplate<String, Pageview> kafkaTemplate, String lateTopic, long windowSizeMs, long graceMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.lateTopic = lateTopic;
        this.windowSizeMs = windowSizeMs;
        this.graceMs = graceMs;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public KeyValue<String, Pageview> transform(String key, Pageview pageview) {
        if (pageview == null) {
            return null;
        }

        long recordTsMs = pageview.timestamp() * 1000L;
        long streamTimeMs = context.currentStreamTimeMs();

        long windowStartMs = (recordTsMs / windowSizeMs) * windowSizeMs;
        long windowEndMs = windowStartMs + windowSizeMs;
        long windowCloseMs = windowEndMs + graceMs;

        if (streamTimeMs >= windowCloseMs) {
            kafkaTemplate.send(lateTopic, pageview.postcode(), pageview);
            return null;
        }

        return KeyValue.pair(key != null ? key : pageview.postcode(), pageview);
    }

    @Override
    public void close() {
        // Do nothing
    }
}
