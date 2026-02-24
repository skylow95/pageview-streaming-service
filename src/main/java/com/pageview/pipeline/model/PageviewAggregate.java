package com.pageview.pipeline.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PageviewAggregate(
        String postcode,
        @JsonProperty("window_start") String windowStart,
        @JsonProperty("pageview_count") long pageviewCount
) {
}
