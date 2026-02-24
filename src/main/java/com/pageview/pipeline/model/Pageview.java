package com.pageview.pipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Pageview(
        @JsonProperty("user_id") int userId,
        String postcode,
        String webpage,
        long timestamp
) {
}
