package com.pageview.pipeline.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.pageview.pipeline.model.Pageview;
import com.pageview.pipeline.model.PageviewAggregate;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class PageviewAggregationTopologyTest {

    private static final String PAGEVIEW_AGGREGATES = "pageview-aggregates";
    private static final String PAGEVIEWS = "pageviews";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Pageview> inputTopic;
    private TestOutputTopic<String, PageviewAggregate> outputTopic;

    private final ObjectMapper objectMapper = JsonMapper.builder()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    @BeforeEach
    void setUp() {
        PageviewAggregationTopology topology = new PageviewAggregationTopology(
                PAGEVIEWS, PAGEVIEW_AGGREGATES, 60, 5, objectMapper);

        StreamsBuilder builder = new StreamsBuilder();
        topology.pageviewAggregationStream(builder);

        testDriver = new TopologyTestDriver(builder.build(), streamConfig());

        inputTopic = testDriver.createInputTopic(PAGEVIEWS, new StringSerializer(), new JsonSerializer<>(objectMapper));

        JsonDeserializer<PageviewAggregate> outputDeserializer = getPageviewAggregateJsonDeserializer();
        outputTopic = testDriver.createOutputTopic(PAGEVIEW_AGGREGATES, new StringDeserializer(), outputDeserializer);
    }

    private JsonDeserializer<PageviewAggregate> getPageviewAggregateJsonDeserializer() {
        JsonDeserializer<PageviewAggregate> outputDeserializer = new JsonDeserializer<>(PageviewAggregate.class, objectMapper);
        outputDeserializer.addTrustedPackages("*");
        outputDeserializer.setRemoveTypeHeaders(true);
        return outputDeserializer;
    }

    private Properties streamConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "pageview-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "build/kafka-streams-test");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    @AfterEach
    void tearDown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    @DisplayName("Aggregates pageviews by postcode and 1-minute window")
    void aggregatesPageviewsByPostcodeAndWindow() {
        long windowStart = 1611662640L;
        long timestampMs = windowStart * 1000L;

        inputTopic.pipeInput("SW19", new Pageview(1, "SW19", "www.example.com/a", windowStart), java.time.Instant.ofEpochMilli(timestampMs));
        inputTopic.pipeInput("SW19", new Pageview(2, "SW19", "www.example.com/b", windowStart + 15), java.time.Instant.ofEpochMilli(timestampMs + 15_000));
        inputTopic.pipeInput("EC1", new Pageview(3, "EC1", "www.example.com/c", windowStart), java.time.Instant.ofEpochMilli(timestampMs));

        testDriver.advanceWallClockTime(Duration.ofMinutes(7));

        List<KeyValue<String, PageviewAggregate>> records = outputTopic.readKeyValuesToList();

        assertThat(records).isNotEmpty();
        List<PageviewAggregate> results = records.stream()
                .map(kv -> kv.value)
                .collect(Collectors.toList());

        assertThat(results).hasSizeGreaterThanOrEqualTo(2);

        Optional<PageviewAggregate> sw19 = results.stream()
                .filter(a -> "SW19".equals(a.postcode()))
                .max(Comparator.comparingLong(PageviewAggregate::pageviewCount));

        Optional<PageviewAggregate> ec1 = results.stream()
                .filter(a -> "EC1".equals(a.postcode()))
                .findFirst();

        assertThat(sw19).isPresent();
        assertThat(sw19.get().pageviewCount()).as("SW19 count").isGreaterThanOrEqualTo(2L);
        assertThat(ec1).isPresent();
        assertThat(ec1.get().pageviewCount()).as("EC1 count").isEqualTo(1L);
    }
}
