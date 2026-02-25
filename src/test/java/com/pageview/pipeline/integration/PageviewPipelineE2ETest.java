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
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

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
@EmbeddedKafka(partitions = 6, topics = {"pageviews", "pageview-aggregates"})
@Testcontainers
@Tag("e2e")
@DirtiesContext
@ActiveProfiles("test")
class PageviewPipelineE2ETest {

    private static final String E2E_APP_NAME = "pageview-pipeline-e2e-" + UUID.randomUUID();
    private static final String BUCKET = "pageview-data";
    private static final String EVENT_TIME_PARTITION = "2021/01/26/12";
    private static final long WINDOW_START = 1611662640L;

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

    @BeforeEach
     void createBucket() {
        try {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        } catch (Exception ex) {
            // Do nothing
        }
    }

    @Autowired
    MockMvc mockMvc;

    @Autowired
    EmbeddedKafkaBroker broker;

    @Autowired
    S3Client s3Client;

    private final ObjectMapper objectMapper = new ObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Test
    @DisplayName("Pipeline aggregates pageviews by postcode and 1-minute window")
    void aggregatesPageviewsByPostcodeAndWindow() throws Exception {
        long flushTimestamp = WINDOW_START + 7 * 60;

        produceTestData("""
                {"user_id":1,"postcode":"SW19","webpage":"www.example.com/a","timestamp":%d}
                """, WINDOW_START);

        produceTestData("""
                {"user_id":2,"postcode":"SW19","webpage":"www.example.com/b","timestamp":%d}
                """, WINDOW_START + 15);

        produceTestData("""
                {"user_id":3,"postcode":"EC1","webpage":"www.example.com/c","timestamp":%d}
                """, WINDOW_START);

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
        assertThat(sw19.get().pageviewCount()).isGreaterThanOrEqualTo(2L);
        assertThat(ec1).isPresent();
        assertThat(ec1.get().pageviewCount()).isEqualTo(1L);

        // Verify raw S3 path
        List<String> rawKeys = await()
                .pollDelay(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(30))
                .until(this::listS3Keys, keys -> !keys.isEmpty());

        assertThat(rawKeys)
                .anyMatch(key -> key.contains("raw/" + EVENT_TIME_PARTITION + "/"));
    }

    private List<String> listS3Keys() {
        ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(BUCKET).prefix("raw/").build();
        ListObjectsV2Response response = s3Client.listObjectsV2(listObjectsV2Request);
        return response.contents().stream().map(S3Object::key).toList();
    }

    private void produceTestData(String x, long windowStart) throws Exception {
        mockMvc.perform(post("/api/pageviews")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(x.formatted(windowStart)))
                .andExpect(status().isAccepted());
    }

    private void consumeAggregatesInto(List<PageviewAggregate> into) {
        String groupId = "e2e-test-" + System.currentTimeMillis();
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
