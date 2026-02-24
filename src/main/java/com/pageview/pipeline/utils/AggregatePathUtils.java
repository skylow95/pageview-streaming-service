package com.pageview.pipeline.utils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Shared path formatting for aggregate S3 writers (AggregateS3Writer, LateEventS3Writer).
 */
public final class AggregatePathUtils {

    private static final DateTimeFormatter PATH_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter WINDOW_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneOffset.UTC);

    private AggregatePathUtils() {
    }

    /**
     * Converts window_start (ISO local date-time, e.g. "2025-02-23T10:04:00") to S3 path segment (yyyy/MM/dd).
     * Falls back to current date if parsing fails.
     */
    public static String pathFromWindowStart(String windowStart) {
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(windowStart, WINDOW_FORMATTER);
            return PATH_FORMATTER.format(localDateTime.atZone(ZoneOffset.UTC));
        } catch (Exception e) {
            return PATH_FORMATTER.format(Instant.now().atZone(ZoneOffset.UTC));
        }
    }
}
