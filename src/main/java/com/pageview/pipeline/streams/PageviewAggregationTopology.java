package com.pageview.pipeline.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pageview.pipeline.model.Pageview;
import com.pageview.pipeline.model.PageviewAggregate;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Configuration
public class PageviewAggregationTopology {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneOffset.UTC);
    public static final String PAGEVIEW_COUNTS = "pageview-counts";

    private final String inputTopic;
    private final String outputTopic;
    private final Duration aggregationWindow;
    private final Duration gracePeriodWindow;
    private final ObjectMapper objectMapper;

    public PageviewAggregationTopology(
            @Value("${pipeline.pageviews-topic:pageviews}") String inputTopic,
            @Value("${pipeline.aggregates-topic:pageview-aggregates}") String outputTopic,
            @Value("${pipeline.stream.window-size-seconds:60}") int windowSizeSeconds,
            @Value("${pipeline.stream.grace-period-minutes:5}") int gracePeriodMinutes,
            @Qualifier("jsonObjectMapper") ObjectMapper objectMapper) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.aggregationWindow = Duration.ofSeconds(windowSizeSeconds);
        this.gracePeriodWindow = Duration.ofMinutes(gracePeriodMinutes);
        this.objectMapper = objectMapper;
    }

    @Bean
    public KStream<String, Pageview> pageviewAggregationStream(StreamsBuilder builder) {
        final JsonSerde<Pageview> pageviewSerde = pageviewSerde();
        final JsonSerde<PageviewAggregate> aggregateSerde = aggregateSerde();

        KStream<String, Pageview> stream = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), pageviewSerde)
                        .withTimestampExtractor(new PageviewTimestampExtractor()));


        stream
                .groupBy((key, value) -> value.postcode(),
                        Grouped.with(Serdes.String(), pageviewSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(aggregationWindow, gracePeriodWindow))
                .count(Materialized.as(PAGEVIEW_COUNTS))
                .toStream()
                .map(PageviewAggregationTopology::windowCountToAggregate)
                .to(outputTopic, Produced.with(Serdes.String(), aggregateSerde));

        return stream;
    }

    private JsonSerde<Pageview> pageviewSerde() {
        JsonSerde<Pageview> serde = new JsonSerde<>(Pageview.class, objectMapper);
        serde.configure(Map.of(JsonDeserializer.VALUE_DEFAULT_TYPE, Pageview.class.getName()), false);
        return serde;
    }

    private JsonSerde<PageviewAggregate> aggregateSerde() {
        JsonSerde<PageviewAggregate> serde = new JsonSerde<>(PageviewAggregate.class, objectMapper);
        serde.configure(Map.of(JsonDeserializer.VALUE_DEFAULT_TYPE, PageviewAggregate.class.getName()), false);
        return serde;
    }

    private static KeyValue<String, PageviewAggregate> windowCountToAggregate(
            Windowed<String> windowKey, Long count) {
        String postcode = windowKey.key();
        String windowStart = FORMATTER.format(
                Instant.ofEpochMilli(windowKey.window().start()).atZone(ZoneOffset.UTC));
        return KeyValue.pair(postcode, new PageviewAggregate(postcode, windowStart, count));
    }
}
