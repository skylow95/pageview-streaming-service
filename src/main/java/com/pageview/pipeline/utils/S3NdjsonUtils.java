package com.pageview.pipeline.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.charset.StandardCharsets;
import java.util.List;

public final class S3NdjsonUtils {

    private S3NdjsonUtils() {
    }

    public static <T> String toNdjson(List<T> items, ObjectMapper objectMapper) throws JsonProcessingException {
        StringBuilder sb = new StringBuilder(items.size() * 128);
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append('\n');
            sb.append(objectMapper.writeValueAsString(items.get(i)));
        }
        return sb.toString();
    }

    public static void writeToS3(S3Client s3Client, String bucketName, String key, String ndJsonValue) {
        s3Client.putObject(builder -> builder.bucket(bucketName).key(key),
                RequestBody.fromString(ndJsonValue, StandardCharsets.UTF_8));
    }
}
