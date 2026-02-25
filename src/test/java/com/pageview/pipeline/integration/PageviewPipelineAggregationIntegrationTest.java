package com.pageview.pipeline.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.pageview.pipeline.model.PageviewAggregate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 6, topics = {"pageviews", "pageview-aggregates", "late-pageview-aggregates"})
@Testcontainers
@Tag("integration")
@DirtiesContext
@ActiveProfiles("test")
class PageviewPipelineAggregationIntegrationTest {

    private static final String E2E_APP_NAME = "pageview-pipeline-integration-" + UUID.randomUUID();

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.0"))
            .withServices(LocalStackContainer.Service.S3)
            .withStartupTimeout(Duration.ofMinutes(3));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("aws.s3.endpoint-override", () -> localstack.getEndpointOverride(LocalStackContainer.Service.S3).toString());
        // Unique app name per run so Kafka Streams gets clean state (no pollution from other tests)
        registry.add("spring.application.name", () -> E2E_APP_NAME);
    }

    @Autowired
    MockMvc mockMvc;

    @Autowired
    EmbeddedKafkaBroker broker;

    @Autowired
    S3Client s3Client;

    private static final String BUCKET = "pageview-data";

    @BeforeEach
    void createBucket() {
        try {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        } catch (software.amazon.awssdk.services.s3.model.S3Exception e) {
            if (!e.awsErrorDetails().errorCode().contains("BucketAlreadyExists")
                    && !e.awsErrorDetails().errorCode().contains("BucketAlreadyOwnedByYou")) {
                throw e;
            }
            // Bucket exists from prior run; ok
        }
    }
    /** Event-time partition for test data (timestamp 1611662640 = 2021-01-26 12:04 UTC). Processing-time would be today's date. */
    private static final String EVENT_TIME_PARTITION = "2021/01/26/12";
    /** Aggregate path for test data (yyyy/MM/dd from window 2021-01-26) */
    private static final String AGGREGATE_DATE_PARTITION = "2021/01/26";

    private final ObjectMapper objectMapper = new ObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    @DisplayName("Pipeline aggregates pageviews by postcode and 1-minute window")
    void aggregatesPageviewsByPostcodeAndWindow() throws Exception {
        long windowStart = 1611662640L;
        long flushTimestamp = windowStart + 7 * 60;

        produceTestData("""
                {"user_id":1,"postcode":"SW19","webpage":"www.example.com/a","timestamp":%d}
                """, windowStart);

        produceTestData("""
                {"user_id":2,"postcode":"SW19","webpage":"www.example.com/b","timestamp":%d}
                """, windowStart + 15);

        produceTestData("""
                {"user_id":3,"postcode":"EC1","webpage":"www.example.com/c","timestamp":%d}
                """, windowStart);

        produceTestData("""
                {"user_id":4,"postcode":"SW19","webpage":"www.example.com/d","timestamp":%d}
                """, flushTimestamp);

        // EC1 window also needs a late event to close (stream time must exceed window-end + grace)
        produceTestData("""
                {"user_id":5,"postcode":"EC1","webpage":"www.example.com/e","timestamp":%d}
                """, flushTimestamp);

        List<PageviewAggregate> accumulated = new ArrayList<>();

        List<PageviewAggregate> aggregates = await()
                .pollDelay(Duration.ofSeconds(5))
                .atMost(Duration.ofSeconds(60))
                .until(() -> {
                    consumeAggregatesInto(accumulated);
                    return accumulated;
                }, list -> list.stream().anyMatch(a -> "SW19".equals(a.postcode()))
                        && list.stream().anyMatch(a -> "EC1".equals(a.postcode())));

        assertThat(aggregates).isNotEmpty();
        // Deduplicate by (postcode, windowStart) - take the one with max count per key
        Map<String, PageviewAggregate> byPostcodeWindow = aggregates.stream()
                .collect(Collectors.toMap(a -> a.postcode() + "|" + a.windowStart(), a -> a,
                        (a, b) -> a.pageviewCount() >= b.pageviewCount() ? a : b));

        Optional<PageviewAggregate> sw19 = byPostcodeWindow.values().stream()
                .filter(a -> "SW19".equals(a.postcode()))
                .max(Comparator.comparingLong(PageviewAggregate::pageviewCount));
        Optional<PageviewAggregate> ec1 = byPostcodeWindow.values().stream()
                .filter(a -> "EC1".equals(a.postcode()))
                .min(Comparator.comparing(PageviewAggregate::windowStart));

        assertThat(sw19).isPresent();
        assertThat(sw19.get().pageviewCount()).as("SW19 count").isGreaterThanOrEqualTo(2L);
        assertThat(ec1).isPresent();
        assertThat(ec1.get().pageviewCount()).as("EC1 count").isEqualTo(1L);

        // Verify raw S3 path uses event-time partitioning, not processing time
        List<String> rawKeys = await()
                .pollDelay(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(30))
                .until(() -> listS3Keys("raw/"), keys -> !keys.isEmpty());
        assertThat(rawKeys)
                .as("Raw S3 path must use event-time partition (yyyy/MM/dd/HH from pageview timestamp), not processing time")
                .anyMatch(key -> key.contains("raw/" + EVENT_TIME_PARTITION + "/"));

        // Verify aggregate files exist in S3
        List<String> aggregateKeys = await()
                .pollDelay(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(30))
                .until(() -> listS3Keys("aggregates/"), keys -> keys.stream().anyMatch(k -> k.contains("aggregates-")));
        assertThat(aggregateKeys)
                .as("Aggregate files must exist under aggregates/yyyy/MM/dd/")
                .anyMatch(key -> key.matches("aggregates/" + AGGREGATE_DATE_PARTITION + "/aggregates-\\d+\\.ndjson"));
        assertThat(contentContainsPostcode(aggregateKeys, "aggregates-", "SW19"))
                .as("Aggregate files must contain SW19 data").isTrue();
        assertThat(contentContainsPostcode(aggregateKeys, "aggregates-", "EC1"))
                .as("Aggregate files must contain EC1 data").isTrue();
    }

    @Test
    @DisplayName("Late events are written to late aggregate files in S3")
    void lateEventsWrittenToS3() throws Exception {
        // Send Event A (12:11) first to close window, then Event B (12:04) - B is late, goes to late-pageviews
        long closeTimestamp = 1611663060L;  // 2021-01-26 12:11:00
        long lateTimestamp = 1611662684L;   // 2021-01-26 12:04:44


        produceTestData("""
                {"user_id":9999,"postcode":"SW19","webpage":"www.example.com/close","timestamp":%d}
                """, closeTimestamp);
        produceTestData("""
                {"user_id":1234,"postcode":"SW19","webpage":"www.website.com/index.html","timestamp":%d}
                """, lateTimestamp);

        // Wait for late aggregates to be written to S3
        List<String> lateKeys = await()
                .pollDelay(Duration.ofSeconds(5))
                .atMost(Duration.ofSeconds(45))
                .until(() -> listS3Keys("aggregates/").stream()
                                .filter(k -> k.contains("late-"))
                                .toList(),
                        keys -> !keys.isEmpty());
        assertThat(lateKeys)
                .as("Late aggregate files must exist under aggregates/yyyy/MM/dd/late-*.ndjson")
                .anyMatch(key -> key.matches("aggregates/" + AGGREGATE_DATE_PARTITION + "/late-\\d+-\\d+\\.ndjson"));
        assertThat(contentContainsPostcode(lateKeys, "late-", "SW19"))
                .as("Late aggregate files must contain the dropped SW19 event").isTrue();
    }

    private List<String> listS3Keys(String prefix) {
        ListObjectsV2Response response = s3Client.listObjectsV2(
                ListObjectsV2Request.builder().bucket(BUCKET).prefix(prefix).build());
        return response.contents().stream().map(S3Object::key).toList();
    }

    private boolean contentContainsPostcode(List<String> keys, String keyFilter, String postcode) throws IOException {
        for (String key : keys) {
            if (!key.contains(keyFilter)) continue;
            String body = new String(
                    s3Client.getObject(GetObjectRequest.builder().bucket(BUCKET).key(key).build()).readAllBytes(),
                    StandardCharsets.UTF_8);
            if (body.contains("\"postcode\":\"" + postcode + "\"")) return true;
        }
        return false;
    }

    private void produceTestData(String x, long windowStart) throws Exception {
        mockMvc.perform(post("/api/pageviews")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(x.formatted(windowStart)))
                .andExpect(status().isAccepted());
    }

    private void consumeAggregatesInto(List<PageviewAggregate> into) {
        String groupId = "integration-test-" + System.currentTimeMillis();
        try (Consumer<String, PageviewAggregate> consumer = getKafkaConsumer(getJsonDeserializer(), groupId)) {
            consumer.subscribe(Collections.singletonList("pageview-aggregates"));
            ConsumerRecords<String, PageviewAggregate> records = consumer.poll(Duration.ofSeconds(2));

            for (ConsumerRecord<String, PageviewAggregate> r : records) {
                into.add(r.value());
            }
        }
    }

    private JsonDeserializer<PageviewAggregate> getJsonDeserializer() {
        JsonDeserializer<PageviewAggregate> deserializer = new JsonDeserializer<>(PageviewAggregate.class, objectMapper);
        deserializer.setRemoveTypeHeaders(true);
        deserializer.addTrustedPackages("*");
        return deserializer;
    }

    private Consumer<String, PageviewAggregate> getKafkaConsumer(JsonDeserializer<PageviewAggregate> deserializer, String groupId) {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                        ConsumerConfig.GROUP_ID_CONFIG, groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class
                ),
                new StringDeserializer(),
                deserializer);
    }
}
