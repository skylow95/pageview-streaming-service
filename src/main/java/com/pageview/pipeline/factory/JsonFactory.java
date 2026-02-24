package com.pageview.pipeline.factory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

/**
 * ObjectMapper configured for Kafka and S3 JSON serialization (ISO-8601 dates, no timestamps).
 */
@Component
public class JsonFactory {

    @Bean("jsonObjectMapper")
    public ObjectMapper jsonObjectMapper() {
        return JsonMapper.builder()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build();
    }
}
