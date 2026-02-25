package com.pageview.pipeline.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 6, topics = {"pageviews", "pageview-aggregates"})
@DirtiesContext
@Tag("integration")
@ActiveProfiles("test")
class PageviewPipelineIntegrationTest {

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("aws.s3.endpoint-override", () -> ""); // Skip LocalStack for fast tests
    }

    @Autowired
    MockMvc mockMvc;

    @Test
    @DisplayName("Application context loads successfully")
    void contextLoads() {
    }

    @Test
    @DisplayName("POST /api/pageviews accepts pageview and returns 202 Accepted")
    void apiAcceptsPageview() throws Exception {
        String body = """
                {"user_id":1234,"postcode":"SW19","webpage":"www.website.com/index.html","timestamp":1611662684}
                """;

        mockMvc.perform(post("/api/pageviews")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted());
    }

    @Test
    @DisplayName("POST /api/pageviews/batch accepts multiple pageviews and returns 202 Accepted")
    void apiAcceptsBatchPageviews() throws Exception {
        String body = """
                [
                  {"user_id":1,"postcode":"SW19","webpage":"www.a.com","timestamp":1611662684},
                  {"user_id":2,"postcode":"EC1","webpage":"www.b.com","timestamp":1611662685}
                ]
                """;

        mockMvc.perform(post("/api/pageviews/batch")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isAccepted());
    }
}
