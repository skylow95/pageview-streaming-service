package com.pageview.pipeline.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PageviewSerializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("Pageview serializes and deserializes correctly")
    void pageviewSerializeDeserialize() throws Exception {
        Pageview pv = new Pageview(1234, "SW19", "www.website.com/index.html", 1611662684L);
        String json = objectMapper.writeValueAsString(pv);
        assertThat(json).contains("\"user_id\":1234");
        assertThat(json).contains("\"postcode\":\"SW19\"");
        assertThat(json).contains("\"webpage\":\"www.website.com/index.html\"");
        assertThat(json).contains("\"timestamp\":1611662684");

        Pageview restored = objectMapper.readValue(json, Pageview.class);
        assertThat(restored).isEqualTo(pv);
    }

    @Test
    @DisplayName("PageviewAggregate serializes and deserializes correctly")
    void pageviewAggregateSerializeDeserialize() throws Exception {
        PageviewAggregate agg = new PageviewAggregate("SW19", "2021-01-26T14:44:00", 42L);
        String json = objectMapper.writeValueAsString(agg);
        assertThat(json).contains("\"postcode\":\"SW19\"");
        assertThat(json).contains("\"window_start\":\"2021-01-26T14:44:00\"");
        assertThat(json).contains("\"pageview_count\":42");

        PageviewAggregate restored = objectMapper.readValue(json, PageviewAggregate.class);
        assertThat(restored).isEqualTo(agg);
    }
}
