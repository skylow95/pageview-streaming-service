package com.pageview.pipeline.streams;

import com.pageview.pipeline.model.Pageview;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Extracts event timestamp from Pageview record for event-time windowing.
 */
public class PageviewTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();
        if (value instanceof Pageview pageview) {
            return pageview.timestamp() * 1000L; // epoch seconds to millis
        }
        return partitionTime;
    }
}
