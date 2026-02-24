package com.pageview.pipeline.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

/**
 * Shared buffer drain utilities for S3 writers.
 */
public final class BufferUtils {

    private BufferUtils() {
    }

    /**
     * Drains up to maxSize items from the queue into a new list. Thread-safe when called
     * from a single consumer (e.g. within synchronized block).
     */
    public static <T> List<T> drain(Queue<T> queue, int maxSize) {
        List<T> batch = new ArrayList<>(maxSize);
        T item;
        while ((item = queue.poll()) != null && batch.size() < maxSize) {
            batch.add(item);
        }
        return Collections.unmodifiableList(batch);
    }
}
